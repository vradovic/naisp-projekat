package sstable

import (
	"fmt"
	"testing"
)

type readTest struct {
	keys []string
	full bool
}

var readTests = []readTest{
	{[]string{"a", "z"}, true},
	{[]string{"ekrem"}, true},
	{[]string{"iv"}, false},
	{[]string{"a", "f"}, true},
	{[]string{"f", "z"}, true},
}

func TestRead(t *testing.T) {
	PATH := "resources\\file_1675452016305441200.db"
	for _, test := range readTests {
		fmt.Println(findByKey(test.keys, PATH, test.full))
	}
}
