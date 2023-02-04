package sstable

import (
	"github.com/vradovic/naisp-projekat/record"
	"os"
	"strings"
)

// Pronalazenje putanja do tabela
func getTables() ([]string, error) {
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

	return files, nil
}

// Citanje ss tabela
func ReadTables(keys []string, full bool) []record.Record {
	tables, err := getTables()
	if err != nil {
		panic(err)
	}

	var records [][]record.Record

	for _, table := range tables {
		data := FindByKey(keys, "resources\\"+table, full)
		records = append(records, data)
	}

	var query []record.Record
	for _, r := range records {
		query = append(query, r...)
	}

	return query
}
