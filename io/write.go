package io

import (
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/structures"
	"github.com/vradovic/naisp-projekat/wal"
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

	return structures.Memtable.Delete(record)
}
