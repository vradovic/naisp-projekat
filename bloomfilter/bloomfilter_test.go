package bloomfilter

import (
	"os"
	"testing"
)

type readTest struct {
	arg      []byte
	expected bool
}

var readTests = []readTest{
	{[]byte("dog"), true},
	{[]byte("nikola"), true},
	{[]byte("Doe"), true},
	{[]byte("god"), true}}

func TestRead(t *testing.T) {
	var bloomFilter = NewBloomFilter(1000, 0.001)
	bloomFilter.Add([]byte("dog"))
	bloomFilter.Add([]byte("god"))
	bloomFilter.Add([]byte("hippo"))
	bloomFilter.Add([]byte("nikola"))
	bloomFilter.Add([]byte("milos"))
	bloomFilter.Add([]byte("vladislav"))
	bloomFilter.Add([]byte("milan"))
	bloomFilter.Add([]byte("mihajlo"))
	bloomFilter.Add([]byte("John"))
	bloomFilter.Add([]byte("Doe"))

	for _, test := range readTests {
		if output := bloomFilter.Read(test.arg); output != test.expected {
			t.Errorf("Got %t, expected %t", output, test.expected)
		}
	}
}

func TestLoad(t *testing.T) {
	var bloomFilter = NewBloomFilter(1000, 0.001)
	bloomFilter.Add([]byte("apple"))
	bloomFilter.Add([]byte("pear"))
	bloomFilter.Add([]byte("orange"))

	filePath := "./test.gob"
	bloomFilter.Save(filePath)

	newB := new(BloomFilter)
	Load(filePath, newB)

	err := os.Remove(filePath)
	if err != nil {
		panic("error while removing file")
	}

	got := newB.Read([]byte("orange"))
	want := true

	if got != want {
		t.Errorf("Got %t, expected %t", got, want)
	}
}
