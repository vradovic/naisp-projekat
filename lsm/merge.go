package lsm

import "os"

// Spajanje dve ss tabele u jednu novu, first i second su putanje do tabela
func MergeTables(first, second string) {
	// ucitaj fajlove
	// redom idi kroz fajlove i izvrsavaj merge
	// napisi novu tabelu

	firstFile, err := os.Open(first)
	if err != nil {
		panic(err)
	}
	defer firstFile.Close()

	secondFile, err := os.Open(first)
	if err != nil {
		panic(err)
	}
	defer secondFile.Close()

	// Pozicioniranje na data segment
}
