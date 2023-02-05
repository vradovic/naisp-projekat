package sstable

import (
	"testing"

	"github.com/vradovic/naisp-projekat/record"
)

func TestWrite(t *testing.T) {
	var allRecords []record.Record
	allRecords = append(allRecords, record.Record{Key: "ana", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 28, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "boban", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 521, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "cone", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 564116, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "danilo", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 51151, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "ekrem", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 46156165, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "franjo", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 35116516, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "goran", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 65468468, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "hamza", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 46581616, Tombstone: true})
	allRecords = append(allRecords, record.Record{Key: "ivan", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 658684684, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "ivana", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 566464, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "jovan", Value: []byte{5, 5, 5, 5, 5, 5, 5, 5}, Timestamp: 35461, Tombstone: false})
	allRecords = append(allRecords, record.Record{Key: "zoki", Value: []byte{1, 1, 1, 1, 1, 1, 1, 1}, Timestamp: 65161, Tombstone: true})
	NewSSTable(&allRecords, 1)
}
