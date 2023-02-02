package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(findByKey([]string{"a", "f"}, "file.db", true))
}
