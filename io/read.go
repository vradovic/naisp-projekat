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

	var result []record.Record

	for _, memRec := range memtableRecords {
		for i, ssRec := range sstableRecords {
			if memRec.Key == ssRec.Key {
				sstableRecords[i] = memRec
			}
		}
	}

	for _, memRec := range memtableRecords {
		if !sstable.ContainsRecord(sstableRecords, memRec) {
			sstableRecords = append(sstableRecords, memRec)
		}
	}

	// Weed out deleted records
	for _, rec := range sstableRecords {
		if !rec.Tombstone {
			result = append(result, rec)
		}
	}

	return result
}

func RangeScan(start, end string) []record.Record {
	memtableRecords := structures.Memtable.RangeScan(start, end)
	sstableRecords := sstable.ReadTables([]string{start, end}, true)

	var result []record.Record

	for _, memRec := range memtableRecords {
		for i, ssRec := range sstableRecords {
			if memRec.Key == ssRec.Key {
				sstableRecords[i] = memRec
			}
		}
	}

	for _, memRec := range memtableRecords {
		if !sstable.ContainsRecord(sstableRecords, memRec) {
			sstableRecords = append(sstableRecords, memRec)
		}
	}

	// Weed out deleted records
	for _, rec := range sstableRecords {
		if !rec.Tombstone {
			result = append(result, rec)
		}
	}

	return result
}
