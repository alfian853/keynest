package main

import (
	"encoding/json"
	"fmt"
	"keynest"
	testcase_gen "keynest/testcase-gen"
	"net/http"
	"time"
)

type Response struct {
	StatusCode int    `json:"status_code,omitempty"`
	Message    string `json:"message,omitempty"`
	Val        any    `json:"val,omitempty"`
}

func main() {
	cluster := keynest.NewTableCluster(&keynest.Config{
		IndexSkipNum:       50,
		WriteBufferSize:    1024 * 4,
		FalsePositiveRate:  0.01,
		Lvl0MaxTableNum:    4,
		CompactionInterval: time.Second * 4,
	})

	http.Handle("/add-records", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		suffix := r.URL.Query().Get("suffix")

		resp := &Response{
			StatusCode: http.StatusOK,
		}
		defer func() {
			if r := recover(); r != nil {
				resp := &Response{
					StatusCode: http.StatusInternalServerError,
					Message:    "internal server error",
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}()
		records := testcase_gen.GenerateRandKeyPairs(1000)
		for i, record := range records {
			records[i].Key = fmt.Sprintf("%s-%s-%s", prefix, record.Key, suffix)
			fmt.Println(records[i])
		}
		cluster.AddRecords(records)
	}))

	http.Handle("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &Response{}
		defer func() {
			if r := recover(); r != nil {
				resp := &Response{
					StatusCode: http.StatusInternalServerError,
					Message:    "internal server error",
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)

		}()
		key := r.URL.Query().Get("key")
		if key == "" {
			resp.Message = "key is required"
			resp.StatusCode = http.StatusBadRequest
			return
		}

		val, ok := cluster.Get(key)
		if !ok {
			resp.StatusCode = http.StatusNotFound
			resp.Message = "key not found"
			return
		}
		resp.StatusCode = http.StatusOK
		resp.Val = val
	}))

	http.ListenAndServe(":8080", nil)
}
