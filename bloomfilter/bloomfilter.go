package bloomfilter

import "math"

type BloomFilter struct {
	M             uint
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

	b := BloomFilter{m, data, hashFunctions}

	return &b
}

// Dodavanje elementa u bloomfilter
// data -> element za dodavanje
func (b BloomFilter) Add(data []byte) {
	for _, hashFunction := range b.HashFunctions {
		hashed := hashFunction.Hash(data)
		bit := hashed % uint64(b.M) // bit u nizu

		targetByte := bit / 8     // bajt u kome se bit nalazi
		bitMask := 1 << (bit % 8) // maska
		index := int(targetByte)
		b.Data[index] = b.Data[index] | byte(bitMask) // bitwise OR kako bi upisali jedinicu
	}
}

// Citanje elementa
// data -> element za citanje
func (b BloomFilter) Read(data []byte) bool {
	for _, hashFunction := range b.HashFunctions {
		// Isto kao kod pisanja
		hashed := hashFunction.Hash(data)
		bit := hashed % uint64(b.M)

		targetByte := bit / 8
		bitMask := 1 << (bit % 8)
		index := int(targetByte)

		// bitwise AND kako bi proverili da li je bit na datoj poziciji
		if b.Data[index]&byte(bitMask) == 0 {
			return false
		}
	}

	return true
}
