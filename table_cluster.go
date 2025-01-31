package keynest

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"
	"sync"
	"time"
)

type TableCluster struct {
	//first dimension is level, second is horizontal partition. L0 is the first level that might contains overlap between partition
	//the L1 and above don't contain overlap between partition
	tables    [][]*FTable
	levelLock []sync.RWMutex
	cfg       *Config
}

func NewTableCluster(cfg *Config) *TableCluster {
	tc := &TableCluster{
		cfg:    cfg,
		tables: make([][]*FTable, 1),
	}
	tc.tables[0] = make([]*FTable, 0)
	tc.levelLock = []sync.RWMutex{{}}
	tc.runCompactionJob()
	return tc
}

func (t *TableCluster) AddRecords(records []Record) {
	t.levelLock[0].Lock()
	t.tables[0] = append(t.tables[0], NewFTableWithUnsortedRecord(0, records, t.cfg))
	t.levelLock[0].Unlock()
}

func (t *TableCluster) Get(key string) (any, bool) {
	t.levelLock[0].RLock()
	lvl0Unlock := false
	defer func() {
		if !lvl0Unlock {
			t.levelLock[0].RUnlock()
		}
	}()
	for i, _ := range t.tables[0] {
		val, ok := t.tables[0][i].Get(key)
		if ok {
			return val, true
		}
	}
	t.levelLock[0].RUnlock()
	lvl0Unlock = true

	for i, _ := range t.tables[1:] {
		i = i + 1 //skip index 0
		t.levelLock[i].RLock()
		defer t.levelLock[i].RUnlock()
		minI, maxI, isOverlap := t.findOverlapTablesRange(i, key, key)

		if !isOverlap {
			continue
		}

		for j := minI; j < maxI; j++ {
			val, ok := t.tables[i][j].Get(key)
			if ok {
				return val, true
			}
		}
	}

	return nil, false
}

func (t *TableCluster) runCompactionJob() {

	go func() {
		for range time.Tick(t.cfg.CompactionInterval) {
			t.compactingLvl0()
		}
	}()

}

func (t *TableCluster) compactingLvl0() {
	if len(t.tables[0]) <= t.cfg.Lvl0MaxTableNum {
		return
	}

	//since the lvl 0 might be appended during compaction,
	//so we need to lock the last index of which the compaction will start from index 0 till the last index
	lastIndex := len(t.tables[0])

	fmt.Printf("[INFO] Start compaction job for %d tables at %d\n", lastIndex, time.Now().UnixMilli())

	minHeap := &MinHeapRecord{}
	heap.Init(minHeap)
	totalRecords := 0
	lvl0MinKey := t.tables[0][0].minKey
	lvl0MaxKey := t.tables[0][0].maxKey

	for i := range lastIndex {
		totalRecords += t.tables[0][i].nRecords
		b := make([]byte, SizeOfMetadata)
		t.tables[0][i].dataFile.ReadAt(b, 0)
		m := Metadata{}
		m.UnMarshal(b)
		b = make([]byte, int(m.KeySize)+int(m.ValSize))
		t.tables[0][i].dataFile.ReadAt(b, SizeOfMetadata)
		r := Record{}
		r.UnMarshalKey(b[:m.KeySize])
		r.UnMarshalVal(b[m.KeySize:])

		heap.Push(minHeap, &HeapRecord{
			Record:     &r,
			FTable:     t.tables[0][i],
			nextOffset: SizeOfMetadata + int64(m.KeySize) + int64(m.ValSize),
		})

		if t.tables[0][i].minKey < lvl0MinKey {
			lvl0MinKey = t.tables[0][i].minKey
		} else if t.tables[0][i].maxKey > lvl0MaxKey {
			lvl0MaxKey = t.tables[0][i].maxKey
		}
	}

	minI, maxI, isOverlap := t.findOverlapTablesRange(1, lvl0MinKey, lvl0MaxKey)
	if isOverlap {
		for i, table := range t.tables[1][minI:maxI] {
			totalRecords += table.nRecords
			b := make([]byte, SizeOfMetadata)
			table.dataFile.ReadAt(b, 0)
			m := Metadata{}
			m.UnMarshal(b)
			b = make([]byte, int(m.KeySize)+int(m.ValSize))
			r := Record{}
			r.UnMarshalKey(b[:m.KeySize])
			r.UnMarshalVal(b[m.KeySize:])
			heap.Push(minHeap, &HeapRecord{
				Record:     &r,
				FTable:     t.tables[1][i],
				nextOffset: SizeOfMetadata + int64(m.KeySize) + int64(m.ValSize),
			})
		}
	}

	minRecordCh := make(chan *HeapRecord)
	go func() {
		defer close(minRecordCh)
		for minHeap.Len() > 0 {
			minRecord := heap.Pop(minHeap).(*HeapRecord)
			minRecordCh <- minRecord
			if minRecord.FTable.sizeInBytes <= minRecord.nextOffset {
				continue
			}
			buf := new(bytes.Buffer)

			_, err := minRecord.Marshal(buf)
			if err != nil {
				fmt.Println("Error happen when minRecord.Marshal")
			}
			b := make([]byte, SizeOfMetadata)
			curOffset := minRecord.nextOffset
			minRecord.FTable.dataFile.ReadAt(b, curOffset)
			metadata := Metadata{}
			metadata.UnMarshal(b)
			curOffset += SizeOfMetadata
			b = make([]byte, int(metadata.KeySize)+int(metadata.ValSize))
			minRecord.FTable.dataFile.ReadAt(b, curOffset)
			r := Record{}
			r.UnMarshalKey(b[:metadata.KeySize])
			r.UnMarshalVal(b[metadata.KeySize:])
			curOffset += int64(metadata.KeySize) + int64(metadata.ValSize)
			heap.Push(minHeap, &HeapRecord{
				Record:     &r,
				FTable:     minRecord.FTable,
				nextOffset: curOffset,
			})
		}
	}()

	fmt.Println("[INFO] New table created")
	ftable := NewFTableWithSortedRecordCh(1, minRecordCh, totalRecords, t.cfg)
	t.levelLock[0].Lock()
	if len(t.levelLock) == 1 {
		t.levelLock = append(t.levelLock, sync.RWMutex{})
		t.tables = append(t.tables, []*FTable{})
	}

	t.levelLock[1].Lock()

	if isOverlap {
		for i := minI; i < maxI; i++ {
			fmt.Printf("[INFO] Destroy table[%d][%d]\n", 1, i)
			t.tables[1][i].Destroy()
		}
		t.tables[1] = append(t.tables[1][:minI], append([]*FTable{ftable}, t.tables[1][maxI:]...)...)
	} else {
		t.tables[1] = append(t.tables[1], ftable)
	}
	fmt.Println("[INFO] New table added at lvl 1")

	for i := 0; i < lastIndex; i++ {
		fmt.Printf("[INFO] Destroy table[%d][%d]\n", 0, i)
		t.tables[0][i].Destroy()
	}
	t.tables[0] = t.tables[0][lastIndex:]
	t.levelLock[1].Unlock()
	t.levelLock[0].Unlock()
	fmt.Printf("[INFO] Compaction job done at %d for %d tables\n", time.Now().UnixMilli(), lastIndex)
}

func (t *TableCluster) findOverlapTablesRange(lvl int, minKey, maxKey string) (minI, maxI int, isOverlap bool) {

	if lvl <= 0 || lvl >= len(t.tables) || len(t.tables[lvl]) == 0 {
		return -1, -1, false
	}

	minI = sort.Search(len(t.tables[lvl]), func(i int) bool {
		return minKey < t.tables[lvl][i].minKey
	})

	maxI = sort.Search(len(t.tables[lvl]), func(i int) bool {
		return maxKey < t.tables[lvl][i].maxKey
	})

	minI = max(minI-1, 0)
	maxI = min(maxI, len(t.tables[lvl])-1)
	if t.tables[lvl][minI].maxKey < minKey {
		minI++
	}

	if t.tables[lvl][maxI].minKey < maxKey {
		maxI++
	}

	return minI, maxI, minI != maxI
}
