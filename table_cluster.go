package keynest

import (
	"bytes"
	"container/heap"
	"fmt"
	"sync"
)

type TableCluster struct {
	//first dimension is level, second is horizontal partition. L0 is the first level that might contains overlap between partition
	//the L1 and above don't contain overlap between partition
	tables    [][]*FTable
	levelLock []sync.Mutex
	cfg       *Config
}

func NewTableCluster(cfg *Config) *TableCluster {
	tc := &TableCluster{
		cfg:    cfg,
		tables: make([][]*FTable, 1),
	}
	tc.tables[0] = make([]*FTable, 0)
	tc.levelLock = []sync.Mutex{{}}
	return tc
}

func (t *TableCluster) AddRecords(records []Record) {

	t.tables[0] = append(t.tables[0], NewFTableWithUnsortedRecord(0, records, t.cfg))

	if len(t.tables[0]) > t.cfg.Lvl0MaxTableNum {
		//TODO compact table
		fmt.Println("TODO compact table")
	}
}

func (t *TableCluster) Get(key string) (any, bool) {
	for i, _ := range t.tables[0] {
		val, ok := t.tables[0][i].Get(key)
		if ok {
			return val, true
		}
	}

	//TODO search in L1 and above
	return nil, false
}

func (t *TableCluster) compacting() {
	t.compactingLvl0()

	//TODO search in L1 and above
}

func (t *TableCluster) compactingLvl0() {
	if len(t.tables[0]) <= t.cfg.Lvl0MaxTableNum {
		return
	}

	minHeap := &MinHeapRecord{}
	heap.Init(minHeap)
	totalRecords := 0

	for i, _ := range t.tables[0] {
		totalRecords += t.tables[0][i].nRecords
		b := make([]byte, SizeOfMetadata)
		t.tables[0][i].dataFile.ReadAt(b, SizeOfMetadata)
		m := Metadata{}
		m.UnMarshal(b)
		b = make([]byte, int(m.KeySize)+int(m.ValSize))
		r := Record{}
		r.UnMarshalKey(b[:m.KeySize])
		r.UnMarshalVal(b[m.KeySize:])

		heap.Push(minHeap, &HeapRecord{
			Record:     &r,
			file:       t.tables[0][i].dataFile,
			nextOffset: SizeOfMetadata + int64(m.KeySize) + int64(m.ValSize),
		})
	}

	minRecordCh := make(chan *HeapRecord)
	go func() {
		defer close(minRecordCh)
		for minHeap.Len() > 0 {
			minRecord := heap.Pop(minHeap).(*HeapRecord)
			minRecordCh <- minRecord
			buf := new(bytes.Buffer)

			_, err := minRecord.Marshal(buf)
			if err != nil {
				fmt.Println("Error happen when minRecord.Marshal")
			}
			b := make([]byte, SizeOfMetadata)
			curOffset := minRecord.nextOffset
			minRecord.file.ReadAt(b, curOffset)
			metadata := Metadata{}
			metadata.UnMarshal(b)
			curOffset += SizeOfMetadata
			b = make([]byte, int(metadata.KeySize)+int(metadata.ValSize))
			minRecord.file.ReadAt(b, curOffset)
			r := Record{}
			r.UnMarshalKey(b[:metadata.KeySize])
			r.UnMarshalVal(b[metadata.KeySize:])
			curOffset += int64(metadata.KeySize) + int64(metadata.ValSize)
			heap.Push(minHeap, &HeapRecord{
				Record:     &r,
				file:       minRecord.file,
				nextOffset: curOffset,
			})
		}
	}()

	ftable := NewFTableWithSortedRecordCh(1, minRecordCh, totalRecords, t.cfg)
	t.levelLock[0].Lock()
	if len(t.levelLock) == 1 {
		t.levelLock = append(t.levelLock, sync.Mutex{})
	}
	t.levelLock[1].Lock()
	t.tables[1] = append(t.tables[1], ftable)
	//TODO compact with level 1
	t.levelLock[1].Unlock()
	clear(t.tables[0]) //TODO remove files
	t.levelLock[0].Unlock()
}
