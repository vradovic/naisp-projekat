package record

// Struktura sloga u memoriji
type Record struct {
	Key       string
	Value     []byte
	Timestamp []byte
	Tombstone bool
}
