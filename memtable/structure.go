package memtable

// Struktura memorijske tabele (SkipList ili B-tree)
type Structure interface {
	Write(r Record) bool
	Read(key string) []byte
	Delete(key string) bool
}
