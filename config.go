package keynest

import "time"

type Config struct {
	IndexSkipNum       int
	WriteBufferSize    int
	FalsePositiveRate  float64
	Lvl0MaxTableNum    int
	CompactionInterval time.Duration
	MemFlushInterval   time.Duration
	MemMaxNum          int
}
