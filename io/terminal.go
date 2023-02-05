package io

import (
	"bufio"
	"fmt"
	"github.com/vradovic/naisp-projekat/lsm"
	"github.com/vradovic/naisp-projekat/tokenBucket"
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

	var bytes []byte

	if isSpecialKey(key) {
		var err error
		bytes, err = serializeStructure(key, value)
		if err != nil {
			panic(err)
		}
	} else {
		bytes = []byte(value)
	}

	return key, bytes
}

func GetRangeScanInput() (string, string) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Start: ")
	scanner.Scan()
	start := scanner.Text()

	fmt.Print("End: ")
	scanner.Scan()
	end := scanner.Text()

	return start, end
}

func Menu() error {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println()
		fmt.Println("----------")
		fmt.Println("1. Write")
		fmt.Println("2. Read")
		fmt.Println("3. Delete")
		fmt.Println("4. List")
		fmt.Println("5. Range scan")
		fmt.Println("6. Compact")
		fmt.Println("x. Exit")
		fmt.Println("----------")
		fmt.Println()

		fmt.Print(">")
		scanner.Scan()

		switch scanner.Text() {
		case "1": // PUT
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
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
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				key, _ := GetInput(false)
				rec := Get(key)
				if rec.Tombstone || rec.Key == "" {
					fmt.Println("Record not found")
				} else {
					fmt.Printf("Record found: %s %s", key, string(rec.Value))
				}
			}

		case "3": // DELETE
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
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

		case "4": // LIST
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				key, _ := GetInput(false)
				records := List(key)
				GetPage(records)
			}

		case "5": // RANGE SCAN
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				start, end := GetRangeScanInput()
				records := RangeScan(start, end)
				GetPage(records)
			}

		case "6": // COMPACT
			err := lsm.SizeTiered()
			if err != nil {
				return err
			}

		case "x": // EXIT
			return nil
		default:
			fmt.Println("Invalid input.")
		}
	}
}
