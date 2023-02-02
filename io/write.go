package io

import (
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/sstable"
	"github.com/vradovic/naisp-projekat/structures"
	"github.com/vradovic/naisp-projekat/wal"
	"os"
	"strings"
)

// PUT (Novi slog / azuriranje sloga)
func Put(key string, value []byte, timestamp int64) bool {
	tombstone := false

	log, err := wal.NewWAL(config.GlobalConfig.WalPath)
	if err != nil {
		return false
	}

	err2 := log.Write([]byte(key), value, timestamp, tombstone)
	if err2 != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	return structures.Memtable.Write(record)
}

// GET (Provera da li postoji slog)
func Get(key string) []byte {
	value := structures.Memtable.Read(key)
	if value != nil {
		return value
	}
	value = structures.Cache.LookForRecord(key)
	if value != nil {
		return value
	}

	vals := ReadTables([]string{key}, true)
	if len(vals) > 0 {
		if len(vals[0]) > 0 {
			value = vals[0][0].Value
			return value
		}
	}

	return nil
}

// DELETE (Brisanje sloga)
func Delete(key string, timestamp int64) bool {
	value := []byte("")
	tombstone := true

	log, err := wal.NewWAL(config.GlobalConfig.WalPath)
	if err != nil {
		return false
	}

	err2 := log.Write([]byte(key), value, timestamp, tombstone)
	if err2 != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	success := structures.Memtable.Delete(record)

	if success {
		structures.Cache.DeleteRecord(record)
	}

	return success
}

// Citanje ss tabela
func ReadTables(keys []string, full bool) [][]record.Record {
	tables, err := getTables()
	if err != nil {
		panic(err)
	}

	var records [][]record.Record

	for _, table := range tables {
		data := sstable.FindByKey(keys, "resources\\"+table, full)
		records = append(records, data)
	}

	return records
}

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
