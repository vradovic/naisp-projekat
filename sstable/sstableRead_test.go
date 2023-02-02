package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(FindByKey([]string{"a11", "f561"}, "file.db", true))
}
