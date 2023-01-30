package structures

import (
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/memtable"
)

// STRUKTURE U MEMORIJI

var Memtable *memtable.Memtable

func Init() {
	Memtable = memtable.NewMemtable(config.GlobalConfig.MemtableSize, config.GlobalConfig.StructureType)
}
