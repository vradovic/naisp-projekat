package io

import (
	"bufio"
	"fmt"
	"os"
)

func GetInput() (string, []byte) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Kljuc: ")
	key, _ := reader.ReadString('\n')

	fmt.Print("Vrednost: ")
	value, _ := reader.ReadString('\n')

	return key, []byte(value)
}
