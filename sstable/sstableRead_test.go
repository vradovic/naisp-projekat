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
	{[]string{"b", "j"}, true},
	{[]string{"ekre"}, false},
	{[]string{"i"}, false},
	{[]string{"a", "f"}, true},
	{[]string{"f", "z"}, true},
}

func TestRead(t *testing.T) {
	PATH := "resources\\file_1675455770199457600.db"
	for _, test := range readTests {
		fmt.Println(findByKey(test.keys, PATH, test.full))
	}
}
