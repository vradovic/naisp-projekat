package io

import (
	"github.com/vradovic/naisp-projekat/globals"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/wal"
)

// PUT (Novi slog / azuriranje sloga)
func Put(key string, value []byte, timestamp int64) bool {
	tombstone := false

	log, err := wal.NewWAL(globals.WalPath)
	if err != nil {
		return false
	}

	err2 := log.Write([]byte(key), value, timestamp, tombstone)
	if err2 != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	return globals.Memtable.Write(record)
}

// DELETE (Brisanje sloga)
func Delete(key string, timestamp int64) bool {
	value := []byte("")
	tombstone := true

	log, err := wal.NewWAL(globals.WalPath)
	if err != nil {
		return false
	}

	err2 := log.Write([]byte(key), value, timestamp, tombstone)
	if err2 != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	return globals.Memtable.Delete(record)
}
