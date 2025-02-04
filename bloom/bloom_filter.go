package bloom

import (
	"github.com/spaolacci/murmur3"
	"math"
)

// BloomFilter structure
type BloomFilter struct {
	BitArray  []bool
	Size      uint
	HashFuncs uint
}

// NewBloomFilter creates a new Bloom Filter
func NewBloomFilter(expectedElements uint, falsePositiveRate float64) *BloomFilter {
	m := optimalSize(expectedElements, falsePositiveRate)
	k := optimalHashFunctions(expectedElements, m)
	return &BloomFilter{
		BitArray:  make([]bool, m),
		Size:      m,
		HashFuncs: k,
	}
}

// optimalSize calculates the required bit array Size (m)
func optimalSize(n uint, p float64) uint {
	return uint(math.Ceil(float64(n) * math.Log(p) / math.Log(1/math.Pow(2, math.Log(2)))))
}

// optimalHashFunctions calculates the number of hash functions (k)
func optimalHashFunctions(n, m uint) uint {
	return uint(math.Round((float64(m) / float64(n)) * math.Log(2)))
}

// murmurHash generates a hash value using MurmurHash3
func (bf *BloomFilter) murmurHash(data []byte, seed uint32) uint {
	hash := murmur3.New64WithSeed(seed)
	hash.Write(data)
	return uint(hash.Sum64() % uint64(bf.Size))
}

// Add inserts an item into the Bloom Filter
func (bf *BloomFilter) Add(item string) {
	data := []byte(item)
	for i := uint32(0); i < uint32(bf.HashFuncs); i++ {
		index := bf.murmurHash(data, i)
		bf.BitArray[index] = true
	}
}

// MightContains checks if an item is in the Bloom Filter with false positive probability
// if it returns false, the item is definitely not in the set
func (bf *BloomFilter) MightContains(item string) bool {
	data := []byte(item)
	for i := uint32(0); i < uint32(bf.HashFuncs); i++ {
		index := bf.murmurHash(data, i)
		if !bf.BitArray[index] {
			return false // Definitely not in the set
		}
	}
	return true // Possibly in the set
}
