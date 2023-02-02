package lsm

import (
	"encoding/binary"
	"github.com/vradovic/naisp-projekat/record"
	"os"
)

// Spajanje dve ss tabele u jednu novu, first i second su putanje do tabela
func MergeTables(first, second string) error {
	// ucitaj fajlove
	// redom idi kroz fajlove i izvrsavaj merge
	// napisi novu tabelu

	firstFile, err := os.Open(first)
	if err != nil {
		return err
	}
	defer firstFile.Close()

	secondFile, err := os.Open(first)
	if err != nil {
		return err
	}
	defer secondFile.Close()

	// Dobavljanje duzine data segmenta
	firstLength, err := getDataSegmentLength(firstFile)
	if err != nil {
		return err
	}

	secondLength, err := getDataSegmentLength(secondFile)
	if err != nil {
		return err
	}

	// Pozicioniranje na data segment (zaglavlje je 32 bajtova)
	_, err = firstFile.Seek(32, 0)
	if err != nil {
		return err
	}

	_, err = secondFile.Seek(32, 0)
	if err != nil {
		return err
	}

	// Redosledna obrada
}

func getDataSegmentLength(f *os.File) (int64, error) {
	b := make([]byte, 8)

	_, err := f.Read(b)
	if err != nil {
		return 0, err
	}

	length := int64(binary.LittleEndian.Uint64(b)) - 32 // Oduzimamo zaglavlje

	return length, nil
}

func sequentialUpdate(first, second *os.File) (record.Record, error) {

}

func bytesToRecord(f *os.File) (record.Record, error) {
	// Struktura: KS(8), VS(8), TIME(8), TB(1), K(...), V(...)
	buffer := make([]byte, 8)
	tombstoneBuffer := make([]byte, 1)

	// Key size
	_, err := f.Read(buffer)
	if err != nil {
		return record.Record{}, err
	}
	keySize := binary.LittleEndian.Uint64(buffer)

	// Value size
	_, err = f.Read(buffer)
	if err != nil {
		return record.Record{}, err
	}
	valueSize := binary.LittleEndian.Uint64(buffer)

	// Timestamp
	_, err = f.Read(buffer)
	if err != nil {
		return record.Record{}, err
	}
	timestamp := binary.LittleEndian.Uint64(buffer)

	// Tombstone
	_, err = f.Read(tombstoneBuffer)
	if err != nil {
		return record.Record{}, err
	}
	tombstoneByte := binary.LittleEndian.Uint64(buffer)
	tombstone := tombstoneByte != 0

	// Key
	keyBuffer := make([]byte, keySize)
	_, err = f.Read(keyBuffer)
	if err != nil {
		return record.Record{}, err
	}
	key := string(binary.LittleEndian.Uint64(keyBuffer))

	// Value
	value := make([]byte, valueSize)
	_, err = f.Read(value)
	if err != nil {
		return record.Record{}, err
	}

	return record.Record{
		Key:       key,
		Value:     value,
		Timestamp: int64(timestamp),
		Tombstone: tombstone}, nil
}
