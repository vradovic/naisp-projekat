package structures

import (
	"errors"
	"os"

	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/memtable"
)

// STRUKTURE U MEMORIJI

var Memtable *memtable.Memtable

func Init() {
	// Pravljenje wal fajla ukoliko ne postoji
	if _, err := os.Stat(config.GlobalConfig.WalPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(config.GlobalConfig.WalPath)
		if err != nil {
			panic("wal file error")
		}
		defer f.Close()
	}

	Memtable = memtable.NewMemtable(config.GlobalConfig.MemtableSize, config.GlobalConfig.StructureType)
}
