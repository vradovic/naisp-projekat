package sstable

import (
	"encoding/binary"
	"os"
	"sort"
	"strings"

	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/record"
)

// Pronalazenje putanja do tabela
func GetTables() ([]string, error) {
	var files []string

	dir, err := os.Open("resources")
	defer dir.Close()
	if err != nil {
		return nil, err
	}

	fileInfo, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	for _, file := range fileInfo {
		if strings.Contains(file.Name(), "file") {
			files = append(files, file.Name())
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i] > files[j]
	})

	return files, nil
}

// Citanje ss tabela
func ReadTables(keys []string, full bool) []record.Record {
	tables, err := GetTables()
	if err != nil {
		panic(err)
	}
	if len(tables) <= 0 {
		return nil
	}

	var records [][]record.Record

	for _, table := range tables {
		data := FindByKey(keys, "resources\\"+table, full)
		records = append(records, data)
	}

	result := mergeData(records)

	return result
}

func mergeData(data [][]record.Record) []record.Record {
	freshTable := data[0] // Najsvezija tabela

	for i := 1; i < len(data); i++ {
		for _, rec := range data[i] {
			if !ContainsRecord(freshTable, rec) {
				freshTable = append(freshTable, rec)
			} else {
				swapNewerRecord(&freshTable, rec)
			}
		}
	}

	var result []record.Record
	for _, rec := range freshTable {
		if !rec.Tombstone {
			result = append(result, rec)
		}
	}

	return result
}

func swapNewerRecord(table *[]record.Record, rec record.Record) {
	for i, r := range *table {
		if rec.Key == r.Key && rec.Timestamp > r.Timestamp {
			(*table)[i] = rec
		}
	}
}

func ContainsRecord(table []record.Record, target record.Record) bool {
	found := false

	for _, rec := range table {
		if rec.Key == target.Key {
			found = true
			break
		}
	}

	return found
}

func CountRecords(path string) int {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	buffer := make([]byte, 8)
	_, err = f.Read(buffer)

	dataSegmentLength := int(binary.LittleEndian.Uint64(buffer)) - 32 // Duzina data segmenta u bajtovima
	byteCounter := 0
	counter := 0
	recordSize := config.GlobalConfig.KeySizeSize + config.GlobalConfig.ValueSizeSize + config.GlobalConfig.TimestampSize + config.GlobalConfig.TombstoneSize

	_, err = f.Seek(32, 0)

	for byteCounter < dataSegmentLength {
		keySizeBuff := make([]byte, config.GlobalConfig.KeySizeSize)
		_, err = f.Read(keySizeBuff)
		if err != nil {
			panic(err)
		}
		keySize := binary.LittleEndian.Uint64(keySizeBuff)

		valueSizeBuff := make([]byte, config.GlobalConfig.ValueSizeSize)
		_, err = f.Read(valueSizeBuff)
		if err != nil {
			panic(err)
		}
		valueSize := binary.LittleEndian.Uint64(valueSizeBuff)

		totalSize := recordSize + int(keySize) + int(valueSize)
		byteCounter += totalSize

		offset := totalSize - config.GlobalConfig.KeySizeSize - config.GlobalConfig.ValueSizeSize
		_, err = f.Seek(int64(offset), 1)
		counter++
	}

	return counter
}
