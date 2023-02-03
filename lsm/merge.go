package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/sstable"
	"math"
	"os"
)

// Spajanje dve ss tabele u jednu novu, first i second su putanje do tabela
func MergeTables(first, second string, level int) error {
	// ucitaj fajlove
	// redom idi kroz fajlove i izvrsavaj merge
	// napisi novu tabelu

	firstFile, err := os.Open(first)
	if err != nil {
		return err
	}

	secondFile, err := os.Open(second)
	if err != nil {
		return err
	}

	// Dobavljanje duzine data segmenta
	firstLength, err := getDataSegmentLength(firstFile)
	if err != nil {
		return err
	}

	secondLength, err := getDataSegmentLength(secondFile)
	if err != nil {
		return err
	}

	// Pozicioniranje na data segment (zaglavlje je 32 bajtova)
	_, err = firstFile.Seek(32, 0)
	if err != nil {
		return err
	}

	_, err = secondFile.Seek(32, 0)
	if err != nil {
		return err
	}

	// Redosledna obrada
	records, err := sequentialUpdate(firstFile, secondFile, firstLength, secondLength)
	if err != nil {
		return err
	}

	firstFile.Close()
	secondFile.Close()

	fmt.Println(records)
	sstable.NewSSTable(&records, level)

	return nil
}

func getDataSegmentLength(f *os.File) (int64, error) {
	b := make([]byte, 8)

	_, err := f.Read(b)
	if err != nil {
		return 0, err
	}

	length := int64(binary.LittleEndian.Uint64(b)) - 32 // Oduzimamo zaglavlje

	return length, nil
}

func sequentialUpdate(first, second *os.File, firstLength, secondLength int64) ([]record.Record, error) {
	var firstReadBytes, secondReadBytes int64 = 0, 0
	var firstRecord, secondRecord record.Record
	records := make([]record.Record, 0)

	var bytes int64
	var err error
	firstRecord, bytes, err = bytesToRecord(first)
	secondRecord, bytes, err = bytesToRecord(second)

	if err != nil {
		return []record.Record{}, err
	}
	secondReadBytes += bytes
	if err != nil {
		return []record.Record{}, err
	}
	firstReadBytes += bytes

	for (firstReadBytes < firstLength || secondReadBytes < secondLength) && !(firstRecord.Key == "~" && secondRecord.Key == "~") {

		// Poredjenje
		if firstRecord.Key == secondRecord.Key {
			if firstRecord.Timestamp > secondRecord.Timestamp && !firstRecord.Tombstone {
				records = append(records, firstRecord)
			} else if firstRecord.Timestamp <= secondRecord.Timestamp && !secondRecord.Tombstone {
				records = append(records, secondRecord)
			}

			if firstReadBytes < firstLength && math.Abs(float64(firstReadBytes-firstLength)) > 25 {
				firstRecord, bytes, err = bytesToRecord(first)
				if err != nil {
					return []record.Record{}, err
				}
				firstReadBytes += bytes
			} else {
				firstRecord.Key = "~"
			}

			if secondReadBytes < secondLength && math.Abs(float64(secondReadBytes-secondLength)) > 25 {
				secondRecord, bytes, err = bytesToRecord(second)
				if err != nil {
					return []record.Record{}, err
				}
				secondReadBytes += bytes
			} else {
				secondRecord.Key = "~"
			}

		} else if firstRecord.Key > secondRecord.Key {
			if !secondRecord.Tombstone {
				records = append(records, secondRecord)
			}

			if secondReadBytes < secondLength && math.Abs(float64(secondReadBytes-secondLength)) > 25 {
				secondRecord, bytes, err = bytesToRecord(second)
				if err != nil {
					return []record.Record{}, err
				}
				secondReadBytes += bytes
			} else {
				secondRecord.Key = "~"
			}

		} else if firstRecord.Key < secondRecord.Key {
			if !firstRecord.Tombstone {
				records = append(records, firstRecord)
			}

			if firstReadBytes < firstLength && math.Abs(float64(firstReadBytes-firstLength)) > 25 {
				firstRecord, bytes, err = bytesToRecord(first)
				if err != nil {
					return []record.Record{}, err
				}
				firstReadBytes += bytes
			} else {
				firstRecord.Key = "~"
			}
		}
	}

	return records, nil
}

func bytesToRecord(f *os.File) (record.Record, int64, error) {
	// Struktura: KS(8), VS(8), TIME(8), TB(1), K(...), V(...)
	buffer := make([]byte, 8)
	tombstoneBuffer := make([]byte, 1)

	// Key size
	_, err := f.Read(buffer)
	if err != nil {
		return record.Record{}, 0, err
	}
	keySize := binary.LittleEndian.Uint64(buffer)

	// Value size
	_, err = f.Read(buffer)
	if err != nil {
		return record.Record{}, 0, err
	}
	valueSize := binary.LittleEndian.Uint64(buffer)

	// Timestamp
	_, err = f.Read(buffer)
	if err != nil {
		return record.Record{}, 0, err
	}
	timestamp := binary.LittleEndian.Uint64(buffer)

	// Tombstone
	_, err = f.Read(tombstoneBuffer)
	if err != nil {
		return record.Record{}, 0, err
	}
	tombstone := tombstoneBuffer[0] != 0

	// Key
	keyBuffer := make([]byte, keySize)
	_, err = f.Read(keyBuffer)
	if err != nil {
		return record.Record{}, 0, err
	}
	key := string(keyBuffer)

	// Value
	value := make([]byte, valueSize)
	_, err = f.Read(value)
	if err != nil {
		return record.Record{}, 0, err
	}

	readBytes := 25 + len(key) + len(value) // 25 je fiksna duzina prvih 4 polja

	return record.Record{
		Key:       key,
		Value:     value,
		Timestamp: int64(timestamp),
		Tombstone: tombstone}, int64(readBytes), nil
}
