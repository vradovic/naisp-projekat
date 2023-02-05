package memtable

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/vradovic/naisp-projekat/wal"

	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/record"
	"github.com/vradovic/naisp-projekat/sstable"
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
	case "btree":
		structure = NewBTree(config.GlobalConfig.BTreeOrder)
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
func (m *Memtable) Flush() error {
	records := m.structure.GetItems() // Uzmi sve elemente iz strukture
	// for _, record := range records {
	// 	fmt.Println(record.Key)
	// }

	sstable.NewSSTable(&records, 1)
	err := os.Truncate(config.GlobalConfig.WalPath, 0) // Resetovanje loga
	if err != nil {
		return err
	}
	fmt.Println("Memtable flushed!")
	return nil
}

func (m *Memtable) Write(r record.Record) bool {
	success := m.structure.Write(r)

	if m.structure.GetSize() >= m.maxSize {
		err := m.Flush()
		if err != nil {
			return false
		}

		switch config.GlobalConfig.StructureType { // Nova struktura
		case "skiplist":
			m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
		case "btree":
			m.structure = NewBTree(config.GlobalConfig.BTreeOrder)
		}
	}

	return success
}

func (m *Memtable) Read(key string) (record.Record, bool) {
	return m.structure.Read(key)
}

func (m *Memtable) Delete(r record.Record) bool {
	success := m.structure.Delete(r)

	if m.structure.GetSize() >= m.maxSize {
		err := m.Flush()
		if err != nil {
			return false
		}

		switch config.GlobalConfig.StructureType { // Nova struktura
		case "skiplist":
			m.structure = NewSkipList(config.GlobalConfig.SkipListHeight)
		case "btree":
			m.structure = NewBTree(config.GlobalConfig.BTreeOrder)
		}
	}

	return success
}

func (m *Memtable) List(prefix string) []record.Record {
	return m.structure.List(prefix)
}

func (m *Memtable) RangeScan(start, end string) []record.Record {
	return m.structure.RangeScan(start, end)
}

func (m *Memtable) recover() error {
	walFile, err := os.Open(config.GlobalConfig.WalPath)
	defer walFile.Close()
	if err != nil {
		return err
	}

	for {
		rec, err := wal.ReadWalRecord(walFile)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var success bool
		if rec.Tombstone {
			success = m.structure.Delete(rec)
		} else {
			success = m.structure.Write(rec)
		}

		if !success {
			return errors.New("recovery fail")
		}
	}

	return nil
}
