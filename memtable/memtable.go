package memtable

import "fmt"

type Memtable struct {
	currentSize uint      // Trenutna velicina
	maxSize     uint      // Maksimalna dozvoljena velicina
	structure   Structure // Struktura podataka (SkipList ili B stablo)
}

func (m Memtable) Flush() {
	fmt.Println("Memtable flushed!")
}

func (m Memtable) Write(r Record) bool {
	success := m.structure.Write(r)

	if success {
		m.currentSize++
	}

	return success
}
