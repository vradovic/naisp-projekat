package structures

import (
	"errors"
	"os"

	"github.com/vradovic/naisp-projekat/cache"

	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/memtable"
	"github.com/vradovic/naisp-projekat/tokenBucket"
)

// STRUKTURE U MEMORIJI

var Memtable *memtable.Memtable
var Cache *cache.Cache
var TokenBucket *tokenBucket.TokenBucket

func Init() {
	// Pravljenje resources foldera ukoliko ne postoji
	if _, err := os.Stat("resources"); os.IsNotExist(err) {
		err := os.Mkdir("resources", 0700)
		if err != nil {
			panic("resources error")
		}
	}

	// Pravljenje wal fajla ukoliko ne postoji
	if _, err := os.Stat(config.GlobalConfig.WalPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(config.GlobalConfig.WalPath)
		if err != nil {
			panic("wal file error")
		}
		defer f.Close()
	}

	Memtable = memtable.NewMemtable(config.GlobalConfig.MemtableSize, config.GlobalConfig.StructureType)
	Cache = cache.NewCache(config.GlobalConfig.CacheCapacity)
	TokenBucket = tokenBucket.NewTokenBucket(config.GlobalConfig.TokenNumber, config.GlobalConfig.TokenRefreshTime)

}
