package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(findByKey([]string{"boban"}, "file_1675373913688347100 (1).db", true))
}
