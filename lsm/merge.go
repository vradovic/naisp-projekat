package lsm

import (
	"encoding/binary"
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

	// Pozicioniranje na data segment
}

func jumpToDataSegment(f *os.File) error {
	b := make([]byte, 8)

	_, err := f.Read(b)
	if err != nil {
		return err
	}

	offset := int64(binary.LittleEndian.Uint64(b))
	f.Seek(offset, 0)

	return nil
}
