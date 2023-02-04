package io

import (
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/sstable"
	"github.com/vradovic/naisp-projekat/structures"
)

// GET (Dobavljanje sloga)
func Get(key string) []byte {
	value := structures.Memtable.Read(key)
	if value != nil {
		return value
	}
	value = structures.Cache.LookForRecord(key)
	if value != nil {
		return value
	}

	query := sstable.ReadTables([]string{key}, true)
	if len(query) <= 0 {
		return nil
	}

	value = query[0].Value

	return value
}

func List(key string) []record.Record {
	memtableRecords := structures.Memtable.List(key)
	sstableRecords := sstable.ReadTables([]string{key}, false)

	for _, memRec := range memtableRecords {
		for i, ssRec := range sstableRecords {
			if memRec.Key == ssRec.Key {
				sstableRecords[i] = memRec
			}
		}
	}

	return sstableRecords
}

func RangeScan(start, end string) []record.Record {
	memtableRecords := structures.Memtable.RangeScan(start, end)
	sstableRecords := sstable.ReadTables([]string{start, end}, true)

	for _, memRec := range memtableRecords {
		for i, ssRec := range sstableRecords {
			if memRec.Key == ssRec.Key {
				sstableRecords[i] = memRec
			}
		}
	}

	return sstableRecords
}
