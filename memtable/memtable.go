package memtable

import (
	"fmt"

	"github.com/vradovic/naisp-projekat/globals"
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
		structure = NewSkipList(globals.SKIP_LIST_MAX_HEIGHT)
	default:
		structure = NewSkipList(globals.SKIP_LIST_MAX_HEIGHT)
	}

	m := Memtable{maxSize, structure}

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

		m.structure = NewSkipList(globals.SKIP_LIST_MAX_HEIGHT)
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

		m.structure = NewSkipList(globals.SKIP_LIST_MAX_HEIGHT)
	}

	return success
}
