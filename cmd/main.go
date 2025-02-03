package main

import (
	"encoding/json"
	"fmt"
	"io"
	"keynest"
	testcase_gen "keynest/testcase-gen"
	"net/http"
	"strconv"
	"strings"
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
		Lvl0MaxTableNum:    0,
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

	http.Handle("/record", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		switch r.Method {
		case http.MethodPut:
			// Read from r.Body
			var data any
			contentType := strings.ToLower(r.Header.Get("Content-Type"))
			switch contentType {
			case "application/json":
				if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
					resp.StatusCode = http.StatusBadRequest
					resp.Message = "invalid request body"
					return
				}
			case "plain-text/string":
				body, err := io.ReadAll(r.Body)
				if err != nil {
					resp.StatusCode = http.StatusBadRequest
					resp.Message = "invalid request body"
					return
				}
				data = string(body)
			case "plain-text/int32", "plain-text/int64":
				body, err := io.ReadAll(r.Body)
				numStr := string(body)
				if err != nil {
					resp.StatusCode = http.StatusBadRequest
					resp.Message = "invalid request body"
					return
				}
				if contentType == "plain-text/int32" {
					data, err = strconv.ParseInt(numStr, 10, 32)
				} else {
					data, err = strconv.ParseInt(numStr, 10, 64)
				}
				if err != nil {
					resp.StatusCode = http.StatusInternalServerError
					resp.Message = "failed to parse"
					return
				}
			}

			cluster.Put(key, data)
			resp.StatusCode = http.StatusOK
		case http.MethodDelete:
			cluster.Delete(key)
			resp.StatusCode = http.StatusOK
		case http.MethodGet:
			val, ok := cluster.Get(key)
			if !ok {
				resp.StatusCode = http.StatusNotFound
				resp.Message = "key not found"
				return
			}
			resp.StatusCode = http.StatusOK
			resp.Val = val
		}
	}))

	http.Handle("/trigger-compact", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		cluster.TriggerCompaction()
	}))

	http.Handle("/trigger-memflush", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		cluster.TriggerMemFlush()
	}))

	http.ListenAndServe(":8080", nil)
}
