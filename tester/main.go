package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	baseURL = "http://localhost:8080/record" // Replace with your actual base URL
)

func isOkByChance(okRate float64) bool {
	if rand.Float64() > okRate {
		return false
	}
	return true
}

type Config struct {
	duplicateSetRate              float64
	existGetRate                  float64
	stopGetTheSameKeyNextTimeRate float64
	existDeleteRate               float64
}

type MapData struct {
	header string
	key    string
	val    any
}

var dataQueue = make(chan *MapData, 1000)
var cfg = Config{
	duplicateSetRate:              0.15,
	existGetRate:                  0.7,
	stopGetTheSameKeyNextTimeRate: 0.4,
	existDeleteRate:               0.3,
}

func (m *MapData) MarshalBinary() ([]byte, error) {
	switch m.header {
	case "application/json":
		return json.Marshal(m.val)
	case "plain-text/int32":
		return []byte(strconv.Itoa(m.val.(int))), nil
	case "plain-text/int64":
		return []byte(strconv.FormatInt(m.val.(int64), 10)), nil
	case "plain-text/string":
		return []byte(m.val.(string)), nil
	default:
		return nil, fmt.Errorf("unsupported header: %s", m.header)
	}
}

var client = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:      10,
		IdleConnTimeout:   30 * time.Second,
		DisableKeepAlives: false,
	},
}

func main() {

	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup

	numRequests := 10000 // Number of requests to simulate
	limiter := make(chan struct{}, 10)

	go func() {
		for mapData := range dataQueue {
			if len(dataQueue) < 10 {
				dataQueue <- mapData
			}
		}
	}()

	for i := 0; i < numRequests; i++ {
		fmt.Println("Request:", i)
		wg.Add(1)
		limiter <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-limiter }()
			operation := rand.Intn(3)
			key, header := "", ""
			var bytes []byte
			var val any

			switch operation {
			case 0:
				if isOkByChance(cfg.duplicateSetRate) {
					select {
					case mapData := <-dataQueue:
						key = mapData.key
					case <-time.After(100 * time.Millisecond):
						break
					}
				}

				if key == "" {
					key = randomString(10 + rand.Intn(20))
				}

				header, bytes, val = randomValue()
				setOperation(key, header, bytes, val)
			case 1:
				var mapData *MapData
				if isOkByChance(cfg.existGetRate) {
					select {
					case mapData = <-dataQueue:
						key = mapData.key
					case <-time.After(100 * time.Millisecond):
						break
					}
					//if !isOkByChance(cfg.stopGetTheSameKeyNextTimeRate) {
					//	dataQueue <- mapData
					//}
				}
				if key == "" {
					key = randomString(10 + rand.Intn(20))
				}
				getOperation(key, mapData)
			case 2:
				fromExisting := false
				if isOkByChance(cfg.existDeleteRate) {
					select {
					case mapData := <-dataQueue:
						key = mapData.key
					case <-time.After(100 * time.Millisecond):
						break
					}
					fromExisting = true
				}

				if key == "" {
					key = randomString(10 + rand.Intn(20))
				}
				deleteOperation(key, fromExisting)
			}
		}()
	}

	wg.Wait()
}

func setOperation(key, header string, valBytes []byte, val any) {
	url := fmt.Sprintf("%s?key=%s", baseURL, key)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewBuffer(valBytes))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}
	req.Header.Set("Content-Type", header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error executing request:", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Set key: %s, Status: %s Header: %s, Value: %v\n", key, resp.Status, header, val)

	if resp.StatusCode == http.StatusOK {
		dataQueue <- &MapData{
			header: header,
			val:    val,
			key:    key,
		} // Remember the key and its value if the set operation is successful
	} else {
		panic("set operation failed")
	}
}

func getOperation(key string, expectedVal *MapData) {
	url := fmt.Sprintf("%s?key=%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error executing request:", err)
		return
	}
	defer resp.Body.Close()

	if expectedVal != nil && resp.StatusCode != http.StatusOK {
		panic("get operation failed for key: " + key)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if expectedVal != nil {
		switch expectedVal.header {
		case "application/json":
			expectedBytes, _ := expectedVal.MarshalBinary()
			if strings.TrimSpace(string(bodyBytes)) != strings.TrimSpace(string(expectedBytes)) {
				panic("expected value not equal to actual value")
			}

		case "plain-text/int32":
		case "plain-text/int64":
			expectedText := strconv.FormatInt(expectedVal.val.(int64), 10)
			if string(bodyBytes) != expectedText {
				panic("expected value not equal to actual value")
			}
		case "plain-text/string":
			if expectedVal.val.(string) != string(bodyBytes) {
				panic("expected value not equal to actual value")
			}
		}
	}
}

func deleteOperation(key string, existing bool) {
	url := fmt.Sprintf("%s?key=%s", baseURL, key)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("got error resp:", err)
		panic(err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		panic("delete operation failed for key: " + key)
	}
	fmt.Println("Delete key:", key, "Status:", resp.Status, "Existing:", existing)
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randomValue() (string, []byte, any) {
	switch rand.Intn(3) {
	case 1:
		//jsonValue, _ := json.Marshal(map[string]string{"key": randomString(5)})
		mapVal := map[string]string{"key": randomString(5)}
		jsonValue, _ := json.Marshal(mapVal)
		return "application/json", jsonValue, mapVal
	case 2:
		if rand.Intn(2) == 0 {
			header := "plain-text/int32"
			randInt := rand.Intn(1000)
			return header, []byte(strconv.Itoa(randInt)), randInt
		} else {
			header := "plain-text/int64"
			randInt := rand.Int63()
			return header, []byte(strconv.FormatInt(randInt, 10)), randInt
		}
	default:
		randStr := randomString(20)
		return "plain-text/string", []byte(randStr), randStr
	}
}
