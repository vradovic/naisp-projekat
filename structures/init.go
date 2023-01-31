package structures

import (
	"github.com/vradovic/naisp-projekat/cache"
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/memtable"
	"github.com/vradovic/naisp-projekat/record"
)

// STRUKTURE U MEMORIJI

var Memtable *memtable.Memtable
var Cache *cache.Cache

func Init() {
	Memtable = memtable.NewMemtable(config.GlobalConfig.MemtableSize, config.GlobalConfig.StructureType)
	Cache = cache.NewCache(config.GlobalConfig.CacheCapacity)
	Cache.AddRecord(record.Record{"Mihajlo", []byte("Alo"), 123, false})

}
