package memtable

// Struktura sloga u memorijskoj tabeli
type Record struct {
	Key       string
	Value     []byte
	Timestamp []byte
	Tombstone bool
}
