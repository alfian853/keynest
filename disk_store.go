package keynest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"slices"
	"sort"
)

var SizeOfMetadata = int64(binary.Size(Metadata{}))

type FTable struct {
	dataFile    *os.File
	indexFile   *os.File
	sizeInBytes uint32
	sparseIndex []Record
}

func NewFTable(records []Record) *FTable {
	ftable := &FTable{}

	slices.SortFunc(records, func(a, b Record) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})

	div := len(records) / 100
	offset := uint32(0)
	ftable.dataFile, _ = os.Create("table.data")
	buf := new(bytes.Buffer)
	for i, r := range records {
		records[i].metadata = Metadata{
			KeySize: uint32(len(r.Key)),
			ValSize: uint32(len(r.Val)),
			Offset:  offset,
		}
		if (i+1)%div == 0 {
			ftable.sparseIndex = append(ftable.sparseIndex, records[i])
		}

		err := records[i].Marshal(buf)
		if err != nil {
			panic(err)
			return nil
		}
		ftable.dataFile.Write(buf.Bytes())
		offset += uint32(buf.Len())
	}

	return ftable
}

func (s *FTable) GetAndUnMarshal(key string, val any) (ok bool, err error) {
	// Binary search in the sparse index
	idx := sort.Search(len(s.sparseIndex), func(i int) bool {
		return s.sparseIndex[i].Key >= key
	})

	// Determine the starting offset
	var startOffset int64
	var maxOffset int64
	if idx < len(s.sparseIndex) && s.sparseIndex[idx].Key == key {
		*val.(*string) = s.sparseIndex[idx].Val
		return true, nil
		//startOffset = int64(s.sparseIndex[idx].metadata.Offset)
		//maxOffset = int64(s.sparseIndex[min(idx+1, len(s.sparseIndex)-1)].metadata.Offset)
	} else if idx > 0 {
		startOffset = int64(s.sparseIndex[idx-1].metadata.Offset)
		maxOffset = int64(s.sparseIndex[min(idx, len(s.sparseIndex)-1)].metadata.Offset)
	}

	// Read the data file starting from the determined offset
	var curOffset = startOffset
	for curOffset < maxOffset {
		// Read metadata
		metadata := Metadata{}
		headerBytes := make([]byte, SizeOfMetadata)
		if _, err := s.dataFile.ReadAt(headerBytes, curOffset); err != nil {
			log.Printf("error reading metadata at offset %d: %v", curOffset, err)
			return false, err
		}
		metadata.UnMarshal(headerBytes)
		curOffset += SizeOfMetadata

		// Read key
		keyBytes := make([]byte, metadata.KeySize)
		if _, err := s.dataFile.ReadAt(keyBytes, curOffset); err != nil {
			return false, err
		}
		curOffset += int64(metadata.KeySize)

		// Read value
		valBytes := make([]byte, metadata.ValSize)
		if _, err := s.dataFile.ReadAt(valBytes, curOffset); err != nil {
			return false, err
		}
		curOffset += int64(metadata.ValSize)

		if bytes.Equal([]byte(key), keyBytes) {
			return true, convertBytesToType(valBytes, val)
		}
	}
	return false, nil
}

func compareAnyWithBytes(anyVar any, bytesSlice []byte) bool {
	switch v := anyVar.(type) {
	case []byte:
		return bytes.Equal(v, bytesSlice)
	case string:
		return bytes.Equal([]byte(v), bytesSlice)
	default:
		return false
	}
}
func convertBytesToType(bytes []byte, target interface{}) error {
	// Get the type of the target variable
	targetType := reflect.TypeOf(target)
	if targetType.Kind() != reflect.Ptr {
		return fmt.Errorf("target must be a pointer")
	}
	targetType = targetType.Elem() // Get the type of the underlying value

	// Handle basic types
	switch targetType.Kind() {
	case reflect.Bool:
		if len(bytes) == 0 {
			return fmt.Errorf("insufficient bytes for bool")
		}
		*target.(*bool) = bytes[0] != 0
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(bytes) < int(targetType.Size()) {
			return fmt.Errorf("insufficient bytes for integer")
		}
		var val int64
		switch targetType.Size() {
		case 1:
			val = int64(bytes[0])
		case 2:
			val = int64(binary.LittleEndian.Uint16(bytes))
		case 4:
			val = int64(binary.LittleEndian.Uint32(bytes))
		case 8:
			val = int64(binary.LittleEndian.Uint64(bytes))
		}
		*target.(*int64) = val
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if len(bytes) < int(targetType.Size()) {
			return fmt.Errorf("insufficient bytes for unsigned integer")
		}
		var val uint64
		switch targetType.Size() {
		case 1:
			val = uint64(bytes[0])
		case 2:
			val = uint64(binary.LittleEndian.Uint16(bytes))
		case 4:
			val = uint64(binary.LittleEndian.Uint32(bytes))
		case 8:
			val = uint64(binary.LittleEndian.Uint64(bytes))
		}
		*target.(*uint64) = val
		return nil
	case reflect.Float32, reflect.Float64:
		if len(bytes) < int(targetType.Size()) {
			return fmt.Errorf("insufficient bytes for float")
		}
		bits := binary.LittleEndian.Uint64(bytes)
		switch targetType.Kind() {
		case reflect.Float32:
			*target.(*float32) = math.Float32frombits(uint32(bits))
		case reflect.Float64:
			*target.(*float64) = math.Float64frombits(bits)
		}
		return nil
	case reflect.String:
		*target.(*string) = string(bytes)
		return nil
	default:
		return fmt.Errorf("unsupported type: %s", targetType)
	}
}
