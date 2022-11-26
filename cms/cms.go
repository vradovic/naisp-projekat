package cms

import (
	"encoding/gob"
	"os"

	"github.com/vradovic/naisp-projekat/util"
)

type Cms struct {
	M             uint // kolone
	K             uint // redovi
	Table         [][]uint64
	HashFunctions []HashWithSeed
}

func NewCms(epsilon, delta float64) *Cms {
	m := CalculateM(epsilon)
	k := CalculateK(delta)

	// Matrica
	table := make([][]uint64, k)
	var i uint
	for i = 0; i < k; i++ {
		table[i] = make([]uint64, m)
	}

	hashFunctions := CreateHashFunctions(k)

	cms := &Cms{m, k, table, hashFunctions}
	return cms
}

// Dodavanje elementa
func (c Cms) Add(data []byte) {
	// Hash funkcija predstavlja red u tabeli, hash po modulu m je broj kolone
	for row, hashFunction := range c.HashFunctions {
		hash := hashFunction.Hash(data)
		col := hash % uint64(c.M)

		c.Table[row][col] += 1
	}
}

// Citanje brojaca elementa
func (c Cms) Read(data []byte) uint64 {
	counters := make([]uint64, c.K)

	for row, hashFunction := range c.HashFunctions {
		hash := hashFunction.Hash(data)
		col := hash % uint64(c.M)

		counters[row] = c.Table[row][col]
	}

	min := util.MinSliceUnsigned(counters)
	return min
}

// Cuvanje
func (c Cms) Save(filePath string) error {
	file, err := os.Create(filePath)

	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(c)
		file.Close()
	}

	return err
}

// Ucitavanje
func Load(filePath string) (*Cms, error) {
	c := &Cms{}

	file, file_err := os.Open(filePath)

	if file_err != nil {
		file.Close()
		return c, file_err
	}

	decoder := gob.NewDecoder(file)
	decode_err := decoder.Decode(c)

	if decode_err != nil {
		file.Close()
		return c, decode_err
	}

	file.Close()
	return c, nil
}
