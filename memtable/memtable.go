package memtable

import (
	"fmt"
	"os"

	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/record"
)

type Memtable struct {
	maxSize   uint      // Maksimalna dozvoljena velicina
	structure Structure // Struktura podataka (SkipList ili B stablo)
}

func NewMemtable(maxSize uint, structureName string) *Memtable {
	var structure Structure

	switch structureName {
	case "skiplist":
		structure = NewSkipList(config.GlobalConfig.SkipListHeight)
	default:
		structure = NewSkipList(config.GlobalConfig.SkipListHeight)
	}

	m := Memtable{maxSize, structure}

	// Proveri da li wal postoji i nije prazan
	// Ako wal postoji i nije prazan, onda memtable treba da se oporavi
	walInfo, err := os.Stat(config.GlobalConfig.WalPath)
	if err != nil {
		panic("Log file error")
	}

	if walInfo.Size() <= 0 {
		// recover
	}

	return &m
}

// FLush na disk
func (m *Memtable) Flush() {
	records := m.structure.GetItems() // Uzmi sve elemente iz strukture
	for _, record := range records {
		fmt.Println(record.Key)
	}

	// TODO: Potrebno flushovati u data fajl
	fmt.Println("Memtable flushed!")
}

func (m *Memtable) Write(r record.Record) bool {
	success := m.structure.Write(r)

	if m.structure.GetSize() >= m.maxSize {
		m.Flush()

		m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
	}

	return success
}

func (m *Memtable) Read(key string) []byte {
	return m.structure.Read(key)
}

func (m *Memtable) Delete(r record.Record) bool {
	success := m.structure.Delete(r)

	if m.structure.GetSize() >= m.maxSize {
		m.Flush()

		m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
	}

	return success
}
