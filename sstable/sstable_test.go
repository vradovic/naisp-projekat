package sstable

import (
	"testing"

	"github.com/vradovic/naisp-projekat/record"
)

func TestWrite(t *testing.T) {
	var allRecords []record.Record
	allRecords = append(allRecords, record.Record{Key: "a11", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 28, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "b123", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 521, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "b315", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 564116, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "d16", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 51151, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "e6541", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 46156165, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "f561", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 35116516, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "g65", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 65468468, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "h68", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 46581616, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "j651", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 658684684, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "k564", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 566464, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "l58", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 35461, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "m57", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 65161, Tombstone: true})
	NewSSTable(&allRecords)
}
