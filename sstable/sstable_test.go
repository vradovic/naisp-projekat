package sstable

import (
	"testing"

	"github.com/vradovic/naisp-projekat/record"
)

func TestWrite(t *testing.T) {
	var allRecords []record.Record
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 28, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 521, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 564116, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 51151, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 46156165, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 35116516, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 65468468, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 46581616, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 658684684, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 566464, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "prvikkkkkkkkkkkkkkkkk", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 35461, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "drug", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 65161, Tombstone: true})
	NewSSTable(&allRecords)
}
