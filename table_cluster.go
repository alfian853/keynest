package keynest

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

type TableCluster struct {
	//first dimension is level, second is horizontal partition. L0 is the first level that might contains overlap between partition
	//the L1 and above don't contain overlap between partition
	memtable     *MemTable
	ftables      [][]*FTable
	ftablesLock  []sync.RWMutex
	memTableLock sync.Mutex
	cfg          *Config
}

func NewTableCluster(cfg *Config) *TableCluster {
	tc := &TableCluster{
		cfg:      cfg,
		ftables:  make([][]*FTable, 1),
		memtable: NewMemTable(),
	}
	tc.ftables[0] = make([]*FTable, 0)
	tc.ftablesLock = []sync.RWMutex{{}}
	tc.runMemTableFlushJob()
	tc.runFTableCompactionJob()
	return tc
}

func (t *TableCluster) AddRecords(records []*Record) {
	if len(records) == 0 {
		return
	}
	t.ftablesLock[0].Lock()
	t.ftables[0] = append(t.ftables[0], NewFTableWithUnsortedRecord(0, records, t.cfg))
	t.ftablesLock[0].Unlock()
}

func (t *TableCluster) Put(key string, val any) {
	t.memTableLock.Lock()
	t.memtable.Put(key, val)
	t.memTableLock.Unlock()
}

func (t *TableCluster) Delete(key string) {
	t.memTableLock.Lock()
	t.memtable.Delete(key)
	t.memTableLock.Unlock()
}

func (t *TableCluster) Get(key string) (any, bool) {
	val, ok := t.memtable.Get(key)
	if ok {
		return val, true
	}

	t.ftablesLock[0].RLock()
	lvl0Unlock := false
	defer func() {
		if !lvl0Unlock {
			t.ftablesLock[0].RUnlock()
		}
	}()
	for i, _ := range t.ftables[0] {
		val, ok := t.ftables[0][i].Get(key)
		if ok {
			return val, true
		}
	}
	t.ftablesLock[0].RUnlock()
	lvl0Unlock = true

	for i, _ := range t.ftables[1:] {
		i = i + 1 //skip index 0
		t.ftablesLock[i].RLock()
		defer t.ftablesLock[i].RUnlock()
		minI, maxI, isOverlap := t.findOverlapTablesRange(i, key, key)

		if !isOverlap {
			continue
		}

		for j := minI; j < maxI; j++ {
			val, ok := t.ftables[i][j].Get(key)
			if ok {
				return val, true
			}
		}
	}

	return nil, false
}

func (t *TableCluster) TriggerCompaction() {
	t.compactingLvl0()
}

func (t *TableCluster) TriggerMemFlush() {
	t.flushMemTableToFTable()
}

func (t *TableCluster) runFTableCompactionJob() {
	go func() {
		for range time.Tick(t.cfg.CompactionInterval) {
			t.compactingLvl0()
		}
	}()
}

func (t *TableCluster) runMemTableFlushJob() {
	go func() {
		for range time.Tick(t.cfg.MemFlushInterval) {
			if t.memtable.tree.Size() > t.cfg.MemMaxNum {
				t.flushMemTableToFTable()
			}
		}
	}()
}

func (t *TableCluster) flushMemTableToFTable() {
	t.memTableLock.Lock()
	defer t.memTableLock.Unlock()
	defer t.SnapshotTableClusterMetadata()

	values := t.memtable.tree.Values()
	if len(values) == 0 {
		return
	}
	keys := t.memtable.tree.Keys()
	sortedRecords := make([]*Record, len(values))
	for i, v := range values {
		memRecord := v.(*MemRecord)
		sortedRecords[i] = &Record{
			Key: keys[i].(string),
			Val: memRecord.val,
			Metadata: Metadata{
				TombStone: memRecord.tombstone,
			},
		}
	}

	t.AddRecords(sortedRecords)
	t.memtable.tree.Clear()
}

func readRecordFromFTable(file *os.File, offset *int64) (*Record, error) {
	record := Record{}
	b := make([]byte, SizeOfMetadata)
	n, err := file.ReadAt(b, *offset)
	if err != nil {
		return nil, err
	}
	*offset += int64(n)
	err = record.Metadata.UnMarshal(b)
	if err != nil {
		return nil, err
	}
	b = make([]byte, record.ContentSize())
	file.ReadAt(b, *offset)
	*offset += int64(record.ContentSize())

	record.UnMarshalKey(b[:record.Metadata.KeySize])
	err = record.UnMarshalVal(b[record.Metadata.KeySize:])
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (t *TableCluster) compactingLvl0() {
	if len(t.ftables[0]) <= t.cfg.Lvl0MaxTableNum {
		return
	}
	defer t.SnapshotTableClusterMetadata()

	//since the lvl 0 might be appended during compaction,
	//so we need to lock the last index of which the compaction will start from index 0 till the last index
	lastIndex := len(t.ftables[0])

	log.Printf("[INFO] Start compaction job for %d ftables at %d\n", lastIndex, time.Now().UnixMilli())

	totalRecords := 0

	for i := range lastIndex {
		totalRecords += t.ftables[0][i].nRecords
	}
	lvl0records := make([]*Record, 0, totalRecords)

	// merge all records from level 0 to lvl0records in sorted order
	offsets := make([]int64, lastIndex)
	curRecords := make([]*Record, lastIndex)
	for {
		var minRecord *Record
		for i := max(lastIndex-1, 0); i >= 0; i-- {
			if offsets[i] >= t.ftables[0][i].sizeInBytes {
				continue
			}
			if curRecords[i] != nil {
				continue
			}
			tmpRecord, err := readRecordFromFTable(t.ftables[0][i].dataFile, &offsets[i])
			if err != nil {
				log.Printf("[ERROR] Error reading record from file: %v\n", err)
				continue
			}
			curRecords[i] = tmpRecord
		}

		var minRecordPointerOfPointer **Record
		for i := max(lastIndex-1, 0); i >= 0; i-- {
			if curRecords[i] == nil {
				continue
			}
			if minRecord == nil {
				minRecord = curRecords[i]
				minRecordPointerOfPointer = &curRecords[i]
				continue
			}

			if minRecord.Key > curRecords[i].Key {
				minRecord = curRecords[i]
				minRecordPointerOfPointer = &curRecords[i]
			} else if minRecord.Key == curRecords[i].Key { //if duplicate found
				curRecords[i] = nil //remove duplicate
			}
		}
		if minRecord == nil {
			break
		}
		lvl0records = append(lvl0records, minRecord)
		minRecord = nil
		*minRecordPointerOfPointer = nil
	}

	var ftable *FTable
	minI, maxI, isOverlap := t.findOverlapTablesRange(1, lvl0records[0].Key, lvl0records[len(lvl0records)-1].Key)
	if isOverlap {
		for _, table := range t.ftables[1][minI:maxI] {
			totalRecords += table.nRecords
		}
		minRecordCh := make(chan *Record)

		go func() {
			defer close(minRecordCh)
			offset := int64(0)
			lvl1Idx := minI
			lvl0Idx := 0
			var lvl1Record *Record

			//merge the sorted records from lvl0 and lvl1
			for lvl1Idx < maxI && lvl0Idx < len(lvl0records) {
				if lvl1Record == nil {
					if offset <= t.ftables[1][lvl1Idx].sizeInBytes {
						tmp, err := readRecordFromFTable(t.ftables[1][lvl1Idx].dataFile, &offset)
						if err != nil {
							fmt.Printf("[ERROR] Error reading record from file: %v\n", err)
							continue
						}
						lvl1Record = tmp
					} else if lvl1Idx+1 < maxI {
						lvl1Idx++
						offset = 0
						continue
					} else {
						break
					}
				}

				if lvl0records[lvl0Idx].Key < lvl1Record.Key {
					minRecordCh <- lvl0records[lvl0Idx]
					lvl0Idx++
				} else if lvl0records[lvl0Idx].Key > lvl1Record.Key {
					minRecordCh <- lvl1Record
					lvl1Record = nil
				} else { //if same, skip lvl1 data as the lvl 0 is the latest one.
					minRecordCh <- lvl1Record
					lvl1Record = nil
					//TODO: provide option strategy to deal with deletion: should we put a tombstone or discard it?
					if !lvl0records[lvl0Idx].TombStone {
						minRecordCh <- lvl0records[lvl0Idx]
					}
					lvl0Idx++
				}
			}

			//accumulate the rest of the records
			for lvl1Idx < maxI { //139266
				if offset < t.ftables[1][lvl1Idx].sizeInBytes {
					lvl1Record = &Record{}
					tmp, err := readRecordFromFTable(t.ftables[1][lvl1Idx].dataFile, &offset)
					if err != nil {
						fmt.Printf("[ERROR] Error reading record from file: %v\n", err)
						continue
					}
					lvl1Record = tmp
					minRecordCh <- lvl1Record
				} else if lvl1Idx+1 < maxI {
					lvl1Idx++
					offset = 0
					continue
				} else {
					break
				}
			}
			for lvl0Idx < len(lvl0records) {
				minRecordCh <- lvl0records[lvl0Idx]
				lvl0Idx++
			}
		}()

		ftable = NewFTableWithSortedRecordCh(1, minRecordCh, totalRecords, t.cfg)

	} else {
		ftable = NewFTableWithUnsortedRecord(1, lvl0records, t.cfg)
	}

	fmt.Println("[INFO] New table created")
	t.ftablesLock[0].Lock()
	if len(t.ftablesLock) == 1 {
		t.ftablesLock = append(t.ftablesLock, sync.RWMutex{})
		t.ftables = append(t.ftables, []*FTable{})
	}

	t.ftablesLock[1].Lock()

	if isOverlap {
		for i := minI; i < maxI; i++ {
			log.Printf("[INFO] Destroy table[%d][%d]\n", 1, i)
			t.ftables[1][i].Destroy()
		}
		t.ftables[1] = append(t.ftables[1][:minI], append([]*FTable{ftable}, t.ftables[1][maxI:]...)...)
	} else {
		t.ftables[1] = append(t.ftables[1], ftable)
	}
	fmt.Println("[INFO] New table added at lvl 1")

	for i := 0; i < lastIndex; i++ {
		log.Printf("[INFO] Destroy table[%d][%d]\n", 0, i)
		t.ftables[0][i].Destroy()
	}
	t.ftables[0] = t.ftables[0][lastIndex:]
	t.ftablesLock[1].Unlock()
	t.ftablesLock[0].Unlock()
	log.Printf("[INFO] Compaction job done at %d for %d ftables\n", time.Now().UnixMilli(), lastIndex)
}

// find overlap tables given a min-max keys. the interpretation of minI-maxI is similar to golang slice [minI-maxI] which means all elements
// from index minI till maxI-1 are included
func (t *TableCluster) findOverlapTablesRange(lvl int, minKey, maxKey string) (minI, maxI int, isOverlap bool) {

	if lvl <= 0 || lvl >= len(t.ftables) || len(t.ftables[lvl]) == 0 {
		return -1, -1, false
	}

	minI = sort.Search(len(t.ftables[lvl]), func(i int) bool {
		return minKey < t.ftables[lvl][i].minKey
	})

	maxI = sort.Search(len(t.ftables[lvl]), func(i int) bool {
		return maxKey < t.ftables[lvl][i].maxKey
	})

	minI = max(minI-1, 0)
	maxI = min(maxI, len(t.ftables[lvl])-1)
	if t.ftables[lvl][minI].maxKey < minKey {
		minI = min(minI+1, len(t.ftables[lvl])-1)
	}

	if t.ftables[lvl][maxI].minKey < maxKey {
		maxI = min(maxI+1, len(t.ftables[lvl])-1)
	}

	isOverlap = t.ftables[lvl][minI].minKey <= minKey && maxKey <= t.ftables[lvl][maxI].maxKey
	if isOverlap && minI == maxI {
		maxI++
	}
	return minI, maxI, isOverlap
}
