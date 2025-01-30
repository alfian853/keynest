package keynest

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"slices"
	"sort"
)

var (
	SizeOfMetadata  = int64(binary.Size(Metadata{}))
	WriteBufferSize = 1024 * 4 // 4KB
)

type FTable struct {
	dataFile    *os.File
	indexFile   *os.File
	sizeInBytes int64
	sparseIndex []Index
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

	div := max(len(records)/100, 1)
	offset := int64(0)
	ftable.dataFile, _ = os.Create("table.data")
	buf := new(bytes.Buffer)
	for i := range records {
		metadata, err := records[i].Marshal(buf)
		if err != nil {
			log.Printf("error marshalling record: %v, Skip the record.", err)
			continue
		}

		if (i+1)%div == 0 {
			ftable.sparseIndex = append(ftable.sparseIndex, Index{
				Key:     records[i].Key,
				KeySize: metadata.KeySize,
				ValSize: metadata.ValSize,
				Offset:  offset,
			})
		}
		offset += SizeOfMetadata + int64(metadata.KeySize) + int64(metadata.ValSize)

		if buf.Len() > WriteBufferSize {
			_, err = ftable.dataFile.Write(buf.Bytes())
			if err != nil {
				log.Printf("error writing to data file: %v", err)
			}
			buf.Reset()
		}
	}
	ftable.sizeInBytes = offset
	if buf.Len() > 0 {
		ftable.dataFile.Write(buf.Bytes())
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
		startOffset = s.sparseIndex[idx].Offset
		idx++
	} else if idx > 0 {
		startOffset = s.sparseIndex[idx-1].Offset
	}
	if idx < len(s.sparseIndex) {
		maxOffset = s.sparseIndex[idx].Offset
	} else {
		maxOffset = s.sizeInBytes
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
		r := Record{}
		r.UnMarshalKey(keyBytes)

		if r.Key == key {
			// Read value
			valBytes := make([]byte, metadata.ValSize)
			if _, err := s.dataFile.ReadAt(valBytes, curOffset); err != nil {
				return false, err
			}
			curOffset += int64(metadata.ValSize)
			r.Val = val
			r.UnMarshalVal(valBytes)
			val = r.Val

			return true, nil
		}
		curOffset += int64(metadata.ValSize)
	}
	return false, nil
}
