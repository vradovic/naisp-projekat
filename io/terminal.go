package io

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/vradovic/naisp-projekat/structures"
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
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println("Nemate pravo na vise zahteva. Molimo sacekajte.")
			} else {
				key, value := GetInput(true)
				timestamp := time.Now().UnixNano()

				success := Put(key, value, timestamp)
				if success {
					fmt.Println("Write saved.")
				} else {
					fmt.Println("Write failed.")
				}
			}

		case "2": // READ
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println("Nemate pravo na vise zahteva. Molimo sacekajte.")
			} else {
				key, _ := GetInput(false)
				value := Get(key)
				if value == nil {
					fmt.Println("Record not found")
				} else {
					fmt.Printf("Record found: %s %s", key, string(value))
				}
			}

		case "3": // DELETE
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println("Nemate pravo na vise zahteva. Molimo sacekajte.")
			} else {
				key, _ := GetInput(false)
				timestamp := time.Now().UnixNano()

				success := Delete(key, timestamp)
				if success {
					fmt.Println("Delete saved.")
				} else {
					fmt.Println("Delete failed.")
				}
			}

			// TODO: Implementirati list i range scan

		case "x": // EXIT
			return nil
		default:
			fmt.Println("Invalid input.")
		}
	}
}
