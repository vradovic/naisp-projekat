package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(findByKey([]string{"a", "z"}, "file_1675378026622882000.db", true))
}
