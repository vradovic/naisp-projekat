package io

import (
	"bufio"
	"fmt"
	"github.com/vradovic/naisp-projekat/bloomfilter"
	"github.com/vradovic/naisp-projekat/cms"
	"github.com/vradovic/naisp-projekat/hll"
	"github.com/vradovic/naisp-projekat/lsm"
	"github.com/vradovic/naisp-projekat/simhash"
	"github.com/vradovic/naisp-projekat/tokenBucket"
	"os"
	"time"

	"github.com/vradovic/naisp-projekat/structures"
)

func GetInput(isNewWrite bool, omitSpecial bool) (string, []byte) {
	scanner := bufio.NewScanner(os.Stdin)
	var key, value string
	value = ""

	for {
		fmt.Print("Key: ")
		scanner.Scan()
		key = scanner.Text()
		if len(key) <= 0 {
			fmt.Println("Empty key")
			continue
		}
		break
	}

	if isNewWrite { // Samo ukoliko je novi zapis
		for {
			fmt.Print("Value: ")
			scanner.Scan()
			value = scanner.Text()
			if len(value) <= 0 {
				fmt.Println("Empty value")
				continue
			}
			break
		}
	}

	var bytes []byte

	if isSpecialKey(key) && !omitSpecial {
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
		fmt.Println("1. Write (! - hyperloglog, ? - cms, # - simhash, % - bloomfilter)")
		fmt.Println("2. Read")
		fmt.Println("3. Delete")
		fmt.Println("4. List")
		fmt.Println("5. Range scan")
		fmt.Println("6. Compact")
		fmt.Println("7. Add to struct")
		fmt.Println("8. Read from struct")
		fmt.Println("9. Compare simhashes")
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
				key, value := GetInput(true, false)

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
				key, _ := GetInput(false, false)
				rec := Get(key)
				if rec.Tombstone || rec.Key == "" {
					fmt.Println("Record not found")
				} else {
					fmt.Print("Record found: ")
					fmt.Println(key)
					fmt.Println(string(rec.Value))
				}
			}

		case "3": // DELETE
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				key, _ := GetInput(false, false)
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
				key, _ := GetInput(false, false)
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

		case "7": // ADD TO STRUCT
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				key, val := GetInput(true, true)
				rec := Get(key)
				if rec.Tombstone || rec.Key == "" {
					fmt.Println("Record not found")
					continue
				}

				var bytes []byte
				timestamp := time.Now().UnixNano()

				switch key[0] {
				case '!':
					h := hll.Load(rec.Value)
					h.Add(val)
					bytes = h.Save()
				case '?':
					c := cms.Load(rec.Value)
					c.Add(val)
					bytes = c.Save()
				case '%':
					b := bloomfilter.Load(rec.Value)
					b.Add(val)
					bytes = b.Save()
				default:
					fmt.Println("Not structure type.")
					continue
				}

				success := Put(key, bytes, timestamp)
				if success {
					fmt.Println("Saved.")
				} else {
					fmt.Println("Failed.")
				}
			}

		case "8": // READ FROM STRUCT
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				key, val := GetInput(true, true)
				rec := Get(key)
				if rec.Tombstone || rec.Key == "" {
					fmt.Println("Record not found")
					continue
				}

				switch key[0] {
				case '!':
					h := hll.Load(rec.Value)
					fmt.Println(fmt.Sprint(h.Count()-12, " elements"))
				case '?':
					c := cms.Load(rec.Value)
					n := c.Read(val)
					fmt.Println(fmt.Sprint(n, " occurrences"))
				case '%':
					b := bloomfilter.Load(rec.Value)
					if b.Read(val) {
						fmt.Println("Maybe exists")
					} else {
						fmt.Println("Does not exist")
					}
				default:
					fmt.Println("Not structure type.")
					continue
				}
			}

		case "9": // SIMHASH
			if !structures.TokenBucket.AddRequest("user") {
				fmt.Println(tokenBucket.FAIL_MSG)
			} else {
				key1, _ := GetInput(false, true)
				rec1 := Get(key1)

				key2, _ := GetInput(false, true)
				rec2 := Get(key2)

				if rec1.Tombstone || rec1.Key == "" || rec2.Tombstone || rec2.Key == "" {
					fmt.Println("Record not found")
				} else if key1[0] != '#' || key2[0] != '#' {
					fmt.Println("Not simhash type")
				} else {
					s1 := simhash.Load(rec1.Value)
					s2 := simhash.Load(rec2.Value)

					distance := s1.Distance(s2)
					fmt.Println(fmt.Sprint("Distance: ", distance))
				}
			}

		case "x": // EXIT
			return nil
		default:
			fmt.Println("Invalid input.")
		}
	}
}
