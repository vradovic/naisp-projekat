package structures

import (
	"github.com/vradovic/naisp-projekat/globals"
	"github.com/vradovic/naisp-projekat/memtable"
)

// STRUKTURE U MEMORIJI

var Memtable *memtable.Memtable

func Init() {
	Memtable = memtable.NewMemtable(globals.MEMTABLE_MAX_SIZE, globals.MEMTABLE_STRUCTURE)
}
