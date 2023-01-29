package memtable

import "github.com/vradovic/naisp-projekat/record"

// Struktura memorijske tabele (SkipList ili B-tree)
type Structure interface {
	Write(r record.Record) bool
	Read(key string) []byte
	Delete(key string) bool
}
