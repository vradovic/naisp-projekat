package sstable

import (
	"fmt"
	"testing"
)

func TestRead(t *testing.T) {
	fmt.Println(findByKey([]string{"m"}, "file_1675449068586085700.db", false))
}
