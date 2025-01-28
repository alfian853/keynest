package main

import (
	"fmt"
	"keynest"
	testcase_gen "keynest/testcase-gen"
)

func main() {
	records := testcase_gen.GenerateRandKeyPairs(1000)
	ftable := keynest.NewFTable(records)
	for _, r := range records {
		res := ""
		ok, err := ftable.GetAndUnMarshal(r.Key, &res)
		fmt.Println(r, ok, res, err)
	}

	records = testcase_gen.GenerateRandKeyPairs(1000)
	for _, r := range records {
		res := ""
		ok, err := ftable.GetAndUnMarshal(r.Key, &res)
		fmt.Println(r, ok, res, err)
	}
}
