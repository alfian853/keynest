package keynest

import "os"

type HeapRecord struct {
	*Record
	file       *os.File
	nextOffset int64
}

type MinHeapRecord []*HeapRecord

func (h MinHeapRecord) Len() int           { return len(h) }
func (h MinHeapRecord) Less(i, j int) bool { return h[i].Key < h[j].Key } // Lexicographic order
func (h MinHeapRecord) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeapRecord) Push(x interface{}) {
	*h = append(*h, x.(*HeapRecord))
}

func (h *MinHeapRecord) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
