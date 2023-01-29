package memtable

import (
	"fmt"

	"github.com/vradovic/naisp-projekat/record"
)

type Memtable struct {
	currentSize uint      // Trenutna velicina
	maxSize     uint      // Maksimalna dozvoljena velicina
	structure   Structure // Struktura podataka (SkipList ili B stablo)
}

func NewMemtable(maxSize uint) *Memtable {
	var currentSize uint = 0
	structure := NewSkipList(5)
	m := Memtable{currentSize, maxSize, structure}

	return &m
}

func (m *Memtable) Flush() {
	fmt.Println("Memtable flushed!")
	// TODO: Potrebno flushovati u data fajl
}

func (m *Memtable) Write(r record.Record) bool {
	success := m.structure.Write(r)

	if success {
		m.currentSize++
	}

	if m.currentSize > m.maxSize {
		m.Flush()
	}

	return success
}

func (m *Memtable) Read(key string) []byte {
	return m.structure.Read(key)
}

func (m *Memtable) Delete(key string) bool {
	return m.structure.Delete(key)
}