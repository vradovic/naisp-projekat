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
	{[]string{"j"}, false},
	{[]string{"a", "f"}, true},
	{[]string{"f", "z"}, true},
}

func TestRead(t *testing.T) {
	PATH := "resources\\file_1675454420293433400.db"
	for _, test := range readTests {
		fmt.Println(findByKey(test.keys, PATH, test.full))
	}
}
