package memtable

import (
	"errors"
	"fmt"
	"io"
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
<<<<<<< HEAD
=======
	case "btree":
		structure = NewBTree(config.GlobalConfig.BTreeOrder)
>>>>>>> 17e4529ea576f06c8f08651d6413e6a9795ce2c5
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

	if walInfo.Size() > 0 {
		err := m.recover()
		if err != nil {
			panic(err)
		}
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

<<<<<<< HEAD
		m.structure = NewSkipList(config.GlobalConfig.SkipListHeight) // Nova struktura
		err := os.Truncate(config.GlobalConfig.WalPath, 0)            // Resetovanje loga
=======
		switch config.GlobalConfig.StructureType { // Nova struktura
		case "skiplist":
			m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
		case "btree":
			m.structure = NewBTree(config.GlobalConfig.BTreeOrder)
		}

		err := os.Truncate(config.GlobalConfig.WalPath, 0) // Resetovanje loga
>>>>>>> 17e4529ea576f06c8f08651d6413e6a9795ce2c5
		if err != nil {
			return false
		}
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

<<<<<<< HEAD
		m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
=======
		switch config.GlobalConfig.StructureType { // Nova struktura
		case "skiplist":
			m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
		case "btree":
			m.structure = NewBTree(config.GlobalConfig.BTreeOrder)
		}
>>>>>>> 17e4529ea576f06c8f08651d6413e6a9795ce2c5
	}

	return success
}

func (m *Memtable) recover() error {
	walFile, err := os.Open(config.GlobalConfig.WalPath)
	if err != nil {
		return err
	}
	defer walFile.Close()

	for {
		b := make([]byte, config.GlobalConfig.MaxEntrySize)
		_, e := walFile.Read(b)
		if e == io.EOF {
			break
		} else if e != nil {
			return e
		}

		record := record.BytesToRecord(b)
		var success bool
		if record.Tombstone {
			success = m.structure.Delete(record)
		} else {
			success = m.structure.Write(record)
		}

		if !success {
			return errors.New("recovery fail")
		}
	}

	return nil
}
