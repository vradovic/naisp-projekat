package memtable

// Struktura zapisa u memoriji
type Record struct {
	Key       string
	Value     []byte
	Timestamp uint
	Tombstone bool
}
