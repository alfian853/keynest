package main

import (
	"fmt"
	"keynest"
	testcase_gen "keynest/testcase-gen"
)

func main() {
	cluster := keynest.NewTableCluster(&keynest.Config{
		IndexSkipNum:      50,
		WriteBufferSize:   1024 * 4,
		FalsePositiveRate: 0.01,
		Lvl0MaxTableNum:   4,
	})

	recordsSlice := make([][]keynest.Record, 2)
	for i := 0; i < 2; i++ {
		recordsSlice[i] = testcase_gen.GenerateRandKeyPairs(1000)
		cluster.AddRecords(recordsSlice[i])
	}

	for _, r := range recordsSlice {

		for _, record := range r {
			var res any
			res, ok := cluster.Get(record.Key)
			fmt.Println(record.Key, ok, res)
		}
	}
}
