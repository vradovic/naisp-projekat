package bloomfilter

import "math"

type BloomFilter struct {
	Data          []byte
	HashFunctions []HashWithSeed
}

// Konstruktor za bloomfilter
// expectedElements -> ocekivani broj elemenata
// falsePositiveRate -> tolerancija na gresku
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate) // broj bitova
	k := CalculateK(expectedElements, m)                 // broj hash funkcija

	hashFunctions := CreateHashFunctions(k) // hash funkcije
	bytesNum := math.Ceil(float64(m) / 8)   // broj bajtova
	data := make([]byte, int(bytesNum))     // niz velicine m

	b := BloomFilter{data, hashFunctions}

	return &b
}
