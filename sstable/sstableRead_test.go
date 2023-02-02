package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(findByKey([]string{"a11", "f561"}, "file.db", true))
}
