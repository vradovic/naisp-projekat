package memtable

import "github.com/vradovic/naisp-projekat/record"

// Struktura memorijske tabele (SkipList ili B-tree)
type Structure interface {
	GetSize() uint
	Write(r record.Record) bool
	Read(key string) (record.Record, bool)
	Delete(r record.Record) bool
	GetItems() []record.Record
	List(prefix string) []record.Record
	RangeScan(start string, finish string) []record.Record
}
