package cms

import (
	"os"
	"testing"
)

type readTest struct {
	arg      []byte
	expected uint64
}

var readTests = []readTest{
	{[]byte("Wind's howling."), 3},
	{[]byte("How do you like that silver?"), 2},
	{[]byte("Place of power, gotta be."), 1}}

func TestRead(t *testing.T) {
	cms := NewCms(0.001, 0.001)
	cms.Add([]byte("Wind's howling."))
	cms.Add([]byte("How do you like that silver?"))
	cms.Add([]byte("Place of power, gotta be."))
	cms.Add([]byte("Wind's howling."))
	cms.Add([]byte("How do you like that silver?"))
	cms.Add([]byte("Wind's howling."))

	for _, test := range readTests {
		if output := cms.Read(test.arg); output != test.expected {
			t.Errorf("Got %d, expected %d", output, test.expected)
		}
	}
}

func TestLoad(t *testing.T) {
	cms := NewCms(0.001, 0.001)
	cms.Add([]byte("Wind's howling."))
	cms.Add([]byte("How do you like that silver?"))
	cms.Add([]byte("Place of power, gotta be."))
	cms.Add([]byte("Wind's howling."))
	cms.Add([]byte("How do you like that silver?"))
	cms.Add([]byte("Wind's howling."))

	filePath := "./test.gob"
	save_err := cms.Save(filePath)
	if save_err != nil {
		panic("error while saving")
	}

	newCms, load_err := Load(filePath)

	err := os.Remove(filePath)
	if err != nil {
		panic("error while removing file")
	}

	if load_err != nil {
		panic("error while loading")
	}

	for _, test := range readTests {
		if output := newCms.Read(test.arg); output != test.expected {
			t.Errorf("Got %d, expected %d", output, test.expected)
		}
	}
}
