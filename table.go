package keynest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"keynest/bloom"
	"log"
	"os"
	"slices"
	"sort"
	"time"
)

var (
	SizeOfMetadata = int64(binary.Size(Metadata{}))
)

type FTable struct {
	dataFile    *os.File
	sizeInBytes int64
	nRecords    int
	sparseIndex []Index
	bloomFilter *bloom.BloomFilter
	cfg         *Config
	minKey      string
	maxKey      string
}

func NewFTableWithUnsortedRecord(lvl int, records []*Record, cfg *Config) *FTable {
	ftable := &FTable{
		cfg:      cfg,
		nRecords: len(records),
	}

	slices.SortFunc(records, func(a, b *Record) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})

	//#1. Write to a file and init sparseIndex
	offset := int64(0)
	ftable.dataFile, _ = os.Create(fmt.Sprintf("%d-%d.kv", lvl, time.Now().UnixMilli()))
	buf := new(bytes.Buffer)
	ftable.minKey = records[0].Key
	ftable.maxKey = records[len(records)-1].Key
	for i := range records {
		ftable.writeRecordToFile(records[i], buf, i, &offset)

	}

	ftable.sizeInBytes = offset
	if buf.Len() > 0 {
		ftable.dataFile.Write(buf.Bytes())
	}

	//#2. init bloom filter
	ftable.bloomFilter = bloom.NewBloomFilter(uint(len(records)), cfg.FalsePositiveRate)
	for _, r := range records {
		ftable.bloomFilter.Add(r.Key)
	}

	return ftable
}

func NewFTableWithSortedRecordCh(lvl int, recordCh chan *Record, nRecords int, cfg *Config) *FTable {
	ftable := &FTable{
		cfg:      cfg,
		nRecords: nRecords,
	}

	//#1. Write to a file and init sparseIndex
	offset := int64(0)
	ftable.dataFile, _ = os.Create(fmt.Sprintf("%d-%d.kv", lvl, time.Now().UnixMilli()))
	buf := new(bytes.Buffer)
	i := 0
	ftable.bloomFilter = bloom.NewBloomFilter(uint(nRecords), cfg.FalsePositiveRate)
	for record := range recordCh {
		ftable.writeRecordToFile(record, buf, i, &offset)
		ftable.bloomFilter.Add(record.Key)

		if i == 0 {
			ftable.minKey = record.Key
			ftable.maxKey = record.Key
		} else {
			ftable.maxKey = record.Key
		}
		i++
	}
	ftable.nRecords = i
	ftable.sizeInBytes = offset
	if buf.Len() > 0 {
		ftable.dataFile.Write(buf.Bytes())
	}

	return ftable
}

func (s *FTable) writeRecordToFile(record *Record, buf *bytes.Buffer, i int, offset *int64) {
	metadata, err := record.Marshal(buf)
	if err != nil {
		log.Printf("error marshalling record: %v, Skip the record.", err)
		return
	}

	if (i+1)%s.cfg.IndexSkipNum == 0 {
		s.sparseIndex = append(s.sparseIndex, Index{
			Tombstone: metadata.TombStone,
			Key:       record.Key,
			KeySize:   metadata.KeySize,
			ValSize:   metadata.ValSize,

			Offset: *offset,
		})
	}
	*offset += SizeOfMetadata + int64(metadata.KeySize) + int64(metadata.ValSize)

	if buf.Len() > s.cfg.WriteBufferSize {
		_, err = s.dataFile.Write(buf.Bytes())
		if err != nil {
			log.Printf("error writing to data file: %v", err)
		}
		buf.Reset()
	}
}

func (s *FTable) Get(key string) (val any, ok bool) {
	if !s.bloomFilter.MightContains(key) {
		return nil, false
	}

	// Binary search in the sparse index
	idx := sort.Search(len(s.sparseIndex), func(i int) bool {
		return s.sparseIndex[i].Key >= key
	})

	// Determine the starting offset
	var startOffset int64
	var maxOffset int64
	if idx < len(s.sparseIndex) && s.sparseIndex[idx].Key == key {
		if s.sparseIndex[idx].Tombstone {
			return nil, false
		}
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
			return nil, false
		}
		metadata.UnMarshal(headerBytes)
		curOffset += SizeOfMetadata

		// Read key
		keyBytes := make([]byte, metadata.KeySize)
		if _, err := s.dataFile.ReadAt(keyBytes, curOffset); err != nil {
			return nil, false
		}
		curOffset += int64(metadata.KeySize)
		r := Record{}
		r.UnMarshalKey(keyBytes)
		if r.Key == key {
			if metadata.TombStone {
				return nil, false
			}
			// Read value
			valBytes := make([]byte, metadata.ValSize)
			if _, err := s.dataFile.ReadAt(valBytes, curOffset); err != nil {
				return nil, false
			}
			err := r.UnMarshalVal(valBytes)
			if err != nil {
				return nil, true
			}

			return r.Val, true
		}
		curOffset += int64(metadata.ValSize)
	}
	return nil, false
}

func (s *FTable) Destroy() {
	s.dataFile.Close()
	os.Remove(s.dataFile.Name())
	clear(s.sparseIndex)
}
