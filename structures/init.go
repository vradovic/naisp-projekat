package structures

import (
	"github.com/vradovic/naisp-projekat/cache"
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/memtable"
)

// STRUKTURE U MEMORIJI

var Memtable *memtable.Memtable
var Cache *cache.Cache

func Init() {
	Memtable = memtable.NewMemtable(config.GlobalConfig.MemtableSize, config.GlobalConfig.StructureType)
	Cache = cache.NewCache(config.GlobalConfig.CacheCapacity)

}
