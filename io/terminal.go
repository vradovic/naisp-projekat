package io

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func GetInput(isNewWrite bool) (string, []byte) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Key: ")
	scanner.Scan()
	key := scanner.Text()

	var value = ""
	if isNewWrite { // Samo ukoliko je novi zapis
		fmt.Print("Value: ")
		scanner.Scan()
		value = scanner.Text()
	}

	return key, []byte(value)
}

func Menu() error {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println()
		fmt.Println("----------")
		fmt.Println("1. Write")
		fmt.Println("2. Read")
		fmt.Println("3. Delete")
		fmt.Println("x. Exit")
		fmt.Println("----------")
		fmt.Println()

		fmt.Print(">")
		scanner.Scan()

		switch scanner.Text() {
		case "1": // PUT
			key, value := GetInput(true)
			timestamp := time.Now().UnixNano()

			success := Put(key, value, timestamp)
			if success {
				fmt.Println("Write saved.")
			} else {
				fmt.Println("Write failed.")
			}
		case "2": // READ
			// TODO: Read path...
			fmt.Println("Reading...")
		case "3": // DELETE
			key, _ := GetInput(false)
			timestamp := time.Now().UnixNano()

			success := Delete(key, timestamp)
			if success {
				fmt.Println("Delete saved.")
			} else {
				fmt.Println("Delete failed.")
			}
		case "x": // EXIT
			return nil
		default:
			fmt.Println("Invalid input.")
		}
	}
}
