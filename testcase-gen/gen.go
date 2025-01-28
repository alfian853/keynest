package testcase_gen

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"keynest"
	"math/big"
	"math/rand"
	"time"
)

func GenerateRandKeyPairs(size int) []keynest.Record {
	records := make([]keynest.Record, size)
	for i := 0; i < size; i++ {
		records[i] = keynest.Record{
			Key: randomString(rand.Intn(5) + 5),
			Val: randomString(rand.Intn(5) + 5),
		}
	}
	return records
}

// Generate a random string of specified length
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		idx, _ := crand.Int(crand.Reader, big.NewInt(int64(len(charset))))
		b[i] = charset[idx.Int64()]
	}
	return string(b)
}

// Generate a random JSON object as a map
func randomJSONObject() map[string]interface{} {
	return map[string]interface{}{
		"name": randomString(5),
		"age":  rand.Intn(50) + 18, // Random age between 18 and 67
		"city": randomString(8),
	}
}

// Generate a random list of integers
func randomIntList(count int) []int {
	list := make([]int, count)
	for i := range list {
		list[i] = rand.Intn(1000) // Random integers between 0 and 999
	}
	return list
}

// Generate a random list of strings
func randomStringList(count int) []string {
	list := make([]string, count)
	for i := range list {
		list[i] = randomString(8) // Random string of length 8
	}
	return list
}

// Generate a random list of JSON objects
func randomJSONList(count int) []map[string]interface{} {
	list := make([]map[string]interface{}, count)
	for i := range list {
		list[i] = randomJSONObject()
	}
	return list
}

// Generate a random key-value pair
func generateRandomKeyValuePair() (string, interface{}) {
	rand.Seed(time.Now().UnixNano())

	// Generate random key
	key := randomString(8) // Random string key

	// Generate random value
	valueType := rand.Intn(6) // Six possible types for value
	var value interface{}
	switch valueType {
	case 0:
		value = rand.Intn(1000) // Integer value
	case 1:
		value = randomString(10) // String value
	case 2:
		value = randomJSONObject() // JSON object
	case 3:
		value = randomIntList(rand.Intn(5) + 1) // List of integers (1-5 items)
	case 4:
		value = randomStringList(rand.Intn(5) + 1) // List of strings (1-5 items)
	case 5:
		value = randomJSONList(rand.Intn(5) + 1) // List of JSON objects (1-5 items)
	}

	return key, value
}

// Helper function to format and stringify the value for printing
func formatValue(value interface{}) string {
	switch v := value.(type) {
	case map[string]interface{}, []map[string]interface{}:
		// Handle JSON object or list of JSON objects
		jsonData, _ := json.MarshalIndent(v, "", "  ")
		return string(jsonData)
	default:
		return fmt.Sprintf("%v", v)
	}
}
