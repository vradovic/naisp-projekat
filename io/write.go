package io

import (
	"github.com/vradovic/naisp-projekat/memtable"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/wal"
)

// PUT (Novi slog i brisanje sloga)
func Write(key string, value []byte, timestamp int64, tombstone bool, log *wal.WAL, table *memtable.Memtable) bool {
	err := log.Write([]byte(key), value, timestamp, tombstone)
	if err != nil {
		return false
	}

	record := record.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}

	return table.Write(record)
}
