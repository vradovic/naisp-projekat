package io

import (
	"github.com/vradovic/naisp-projekat/memtable"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/wal"
)

// PUT (Novi slog / azuriranje sloga)
func Put(key string, value []byte, timestamp int64, log *wal.WAL, table *memtable.Memtable) bool {
	tombstone := false
	err := log.Write([]byte(key), value, timestamp, tombstone)
	if err != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	return table.Write(record)
}

// DELETE (Brisanje sloga)
func Delete(key string, timestamp int64, log *wal.WAL, table *memtable.Memtable) bool {
	value := []byte("")
	tombstone := true
	err := log.Write([]byte(key), value, timestamp, tombstone)
	if err != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	return table.Delete(record)
}
