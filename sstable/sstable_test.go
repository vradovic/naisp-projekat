package sstable

import (
	"testing"

	"github.com/vradovic/naisp-projekat/record"
)

func TestWrite(t *testing.T) {
	var allRecords []record.Record
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: []byte{2, 2, 2, 2, 2, 2, 2, 2}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: []byte{3, 3, 3, 3, 3, 3, 3, 3}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: []byte{2, 2, 2, 2, 2, 2, 2, 2}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: []byte{3, 3, 3, 3, 3, 3, 3, 3}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: []byte{2, 2, 2, 2, 2, 2, 2, 2}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: []byte{3, 3, 3, 3, 3, 3, 3, 3}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: []byte{2, 2, 2, 2, 2, 2, 2, 2}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: []byte{3, 3, 3, 3, 3, 3, 3, 3}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: []byte{2, 2, 2, 2, 2, 2, 2, 2}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: []byte{3, 3, 3, 3, 3, 3, 3, 3}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: []byte{2, 2, 2, 2, 2, 2, 2, 2}, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: []byte{3, 3, 3, 3, 3, 3, 3, 3}, Tombstone: true})
	NewSSTable(&allRecords)
}
