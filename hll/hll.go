package hll

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"hash/fnv"
	"math"
	"math/bits"
	"math/rand"
	"time"
)

type HyperLogLog struct {
	Registers []int
	M         uint // number of registers
	B         uint // bits to calculate [4..16]
}

func NewHyperLogLog(m uint) HyperLogLog {
	return HyperLogLog{
		Registers: make([]int, m),
		M:         m,
		B:         uint(math.Ceil(math.Log2(float64(m)))),
	}
}

func countDistinct(input []int) int {
	m := map[int]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}

func leftmostActiveBit(x uint32) int {
	return 1 + bits.LeadingZeros32(x)
}

// create a 32-bit hash
func createHash(stream []byte) uint32 {
	h := fnv.New32()
	h.Write(stream)
	sum := h.Sum32()
	h.Reset()
	return sum
}

func (h HyperLogLog) Add(data []byte) HyperLogLog {
	x := createHash(data)
	k := 32 - h.B // first b bits
	r := leftmostActiveBit(x << h.B)
	j := x >> uint(k)

	if r > h.Registers[j] {
		h.Registers[j] = r
	}
	return h
}

func (h HyperLogLog) Count() uint64 {
	sum := 0.
	m := float64(h.M)
	for _, v := range h.Registers {
		sum += math.Pow(math.Pow(2, float64(v)), -1)
	}
	estimate := .79402 * m * m / sum
	return uint64(estimate)
}

func getRandomData() (out [][]byte, intout []uint32) {
	for i := 0; i < math.MaxInt16; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Uint32()
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, i)
		out = append(out, b)
		intout = append(intout, i)
	}
	return
}

func classicCountDistinct(input []uint32) int {
	m := map[uint32]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}

func (h HyperLogLog) Save() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(h)

	return buffer.Bytes()
}

func Load(data []byte) *HyperLogLog {
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)

	h := &HyperLogLog{}
	err := decoder.Decode(h)
	if err != nil {
		panic("error while decoding")
	}

	return h
}
