package memtable

type Record struct {
	Key       string
	Value     []byte
	Timestamp uint
	Tombstone bool
}
