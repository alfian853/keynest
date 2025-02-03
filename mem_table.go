package keynest

import rbt "github.com/emirpasic/gods/trees/redblacktree"

type MemTable struct {
	tree *rbt.Tree
}

type MemRecord struct {
	tombstone bool
	val       any
}

func NewMemTable() *MemTable {
	return &MemTable{
		tree: rbt.NewWithStringComparator(),
	}
}

func (m *MemTable) Put(key string, val any) {
	m.tree.Put(key, &MemRecord{
		tombstone: false,
		val:       val,
	})
}

func (m *MemTable) Get(key string) (any, bool) {
	record, ok := m.tree.Get(key)
	if ok {
		memRecord := record.(*MemRecord)
		if memRecord.tombstone {
			return nil, false
		}
		return record.(*MemRecord).val, true
	}
	return nil, false
}

func (m *MemTable) Delete(key string) {
	m.tree.Put(key, &MemRecord{
		tombstone: true,
		val:       nil,
	})
}
