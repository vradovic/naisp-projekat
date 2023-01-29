package memtable

// Struktura zapisa u memorijskoj tabeli
type Record struct {
	Key       string
	Value     []byte
	Timestamp uint
	Tombstone bool
}
