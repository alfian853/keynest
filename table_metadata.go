package keynest

import (
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"keynest/bloom"
	"os"
	"sync"
)

type TableClusterMetadata struct {
	FTableMetadata [][]TableMetadata
}

type TableMetadata struct {
	FileName    string
	NRecords    int
	SizeInBytes int64
	MinKey      string
	MaxKey      string
	BloomFilter bloom.BloomFilter
	SparseIndex []Index
}

func (t *TableCluster) SnapshotTableClusterMetadata() {

	clusterMetadata := TableClusterMetadata{}

	for i, _ := range t.ftables {
		clusterMetadata.FTableMetadata = append(clusterMetadata.FTableMetadata, []TableMetadata{})
		for j, _ := range t.ftables[i] {
			clusterMetadata.FTableMetadata[i] = append(clusterMetadata.FTableMetadata[i], t.ftables[i][j].GetSnapshotTableMetadata())
		}
	}

	bytes, err := msgpack.Marshal(clusterMetadata)
	if err != nil {
		fmt.Printf("[ERROR] Error marshalling cluster metadata: %v\n", err)
		return
	}

	file, err := os.Create(fmt.Sprintf("master-metadata"))
	if err != nil {
		fmt.Printf("[ERROR] Error creating file: %v\n", err)
		return
	}
	defer file.Close()
	_, err = file.Write(bytes)
	if err != nil {
		fmt.Printf("[ERROR] Error writing to file: %v\n", err)
	}
}

func (t *TableCluster) LoadTableClusterMetadata() {
	file, err := os.OpenFile("master-metadata", os.O_RDONLY, 0644)
	defer file.Close()
	if err != nil {
		fmt.Printf("[ERROR] Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	clusterMetadata := TableClusterMetadata{}
	decoder := msgpack.NewDecoder(file)
	err = decoder.Decode(&clusterMetadata)
	if err != nil {
		fmt.Printf("[ERROR] Error decoding file: %v\n", err)
		return
	}

	t.ftablesLock = make([]sync.RWMutex, len(clusterMetadata.FTableMetadata))
	t.ftables = make([][]*FTable, len(clusterMetadata.FTableMetadata))
	for i, _ := range clusterMetadata.FTableMetadata {
		t.ftables[i] = make([]*FTable, len(clusterMetadata.FTableMetadata[i]))
		t.ftablesLock[i] = sync.RWMutex{}
		for j, _ := range clusterMetadata.FTableMetadata[i] {
			dataFile, _ := os.Open(clusterMetadata.FTableMetadata[i][j].FileName)
			t.ftables[i][j] = &FTable{
				cfg:         t.cfg,
				nRecords:    clusterMetadata.FTableMetadata[i][j].NRecords,
				sizeInBytes: clusterMetadata.FTableMetadata[i][j].SizeInBytes,
				minKey:      clusterMetadata.FTableMetadata[i][j].MinKey,
				maxKey:      clusterMetadata.FTableMetadata[i][j].MaxKey,
				dataFile:    dataFile,
				bloomFilter: &clusterMetadata.FTableMetadata[i][j].BloomFilter,
				sparseIndex: clusterMetadata.FTableMetadata[i][j].SparseIndex,
			}
		}
	}
}
