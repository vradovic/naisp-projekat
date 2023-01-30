package globals

import (
	"path/filepath"

	"github.com/vradovic/naisp-projekat/memtable"
)

// GLOBAL
var Memtable *memtable.Memtable
var WalPath string

func init() { // TODO: parametre ucitavati iz config.yaml
	Memtable = memtable.NewMemtable(50, "skiplist")
	WalPath = filepath.Join("resources", "wal.log")
}
