package main

import (
	"encoding/json"
	"fmt"
	"io"
	"keynest"
	testcase_gen "keynest/testcase-gen"
	"log"
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
		IndexSkipNum:       2,
		WriteBufferSize:    1024 * 4,
		FalsePositiveRate:  0.01,
		Lvl0MaxTableNum:    4,
		MemMaxNum:          1000,
		CompactionInterval: time.Second * 4,
		MemFlushInterval:   time.Second * 2,
	})
	mux := http.NewServeMux()

	mux.Handle("/add-records", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	mux.Handle("/record", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}()
		key := r.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
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
					w.WriteHeader(http.StatusBadRequest)
					w.Header().Set("reason", "invalid request body")
					return
				}
			case "plain-text/string":
				body, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Header().Set("reason", "invalid request body")
					return
				}
				data = string(body)
			case "plain-text/int32", "plain-text/int64":
				body, err := io.ReadAll(r.Body)
				numStr := string(body)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Header().Set("reason", "invalid request body")
					return
				}
				if contentType == "plain-text/int32" {
					data, err = strconv.ParseInt(numStr, 10, 32)
				} else {
					data, err = strconv.ParseInt(numStr, 10, 64)
				}
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					w.Header().Set("reason", "failed to parse")
					return
				}
			}

			cluster.Put(key, data)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			cluster.Delete(key)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			val, ok := cluster.Get(key)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			switch val.(type) {
			case int64:
				w.Header().Set("Content-Type", "plain-text/int64")
				w.Write([]byte(strconv.FormatInt(val.(int64), 10)))
			case int32:
				w.Header().Set("Content-Type", "plain-text/int32")
				w.Write([]byte(strconv.FormatInt(int64(val.(int32)), 10)))
			case string:
				w.Header().Set("Content-Type", "plain-text/string")
				w.Write([]byte(val.(string)))
			case any:
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(val)
			}
		}
	}))

	mux.Handle("/trigger-compact", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	mux.Handle("/trigger-memflush", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	mux.Handle("/trigger-snapshot", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		cluster.SnapshotTableClusterMetadata()
	}))

	mux.Handle("/trigger-load-metadata", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

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

		cluster.LoadTableClusterMetadata()
	}))

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		IdleTimeout:  10 * time.Second,
	}

	// Handle panics
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovered from panic: %v", err)
		}
	}()

	log.Println("Starting server on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Could not listen on :8080: %v", err)
	}
}
