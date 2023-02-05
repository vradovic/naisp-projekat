package sstable

import (
	"os"
	"sort"
	"strings"

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
