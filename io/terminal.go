package io

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func GetInput(isNewWrite bool) (string, []byte) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Kljuc: ")
	key, _ := reader.ReadString('\n')

	var value = ""
	if isNewWrite { // Samo ukoliko je novi zapis
		fmt.Print("Vrednost: ")
		value, _ = reader.ReadString('\n')
	}

	return key, []byte(value)
}

func Menu() error {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println()
		fmt.Println('-' * 10)
		fmt.Println("1. Write")
		fmt.Println("2. Read")
		fmt.Println("3. Delete")
		fmt.Println("x. Exit")
		fmt.Println('-' * 10)
		fmt.Println()

		fmt.Print('>')
		input, _, err := reader.ReadRune()
		if err != nil {
			return err
		}

		switch input {
		case '1': // PUT
			key, value := GetInput(true)
			timestamp := time.Now().UnixNano()

			success := Put(key, value, timestamp)
			if success {
				fmt.Println("Write saved.")
			} else {
				fmt.Println("Write failed.")
			}
		case '2':
			// TODO: Read path...
			fmt.Println("Reading...")
		case '3':
			key, _ := GetInput(false)
			timestamp := time.Now().UnixNano()

			success := Delete(key, timestamp)
			if success {
				fmt.Println("Delete saved.")
			} else {
				fmt.Println("Delete failed.")
			}
		case 'x':
			return nil
		default:
			fmt.Println("Invalid input.")
		}
	}
}
