package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(findByKey([]string{"a"}, "file_1675436573082509000.db", false))
}
