package bloomfilter

import "testing"

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
	var bloomFilter = NewBloomFilter(1000, 0.01)
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
