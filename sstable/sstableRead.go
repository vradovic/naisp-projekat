package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"

	"github.com/vradovic/naisp-projekat/bloomfilter"
)

func findByKey(key string, path string) string {
	f, err := os.OpenFile(path, os.O_RDONLY, 0600) // unece se jos jedan parametar strukture SSTable za ime fajla
	if err != nil {
		panic(err)
	}

	maybeInFile := checkBloomFilter(f, key)
	defer f.Close()

	if maybeInFile {
		// idi u summary
		result := checkSummary(f, key)
		if result != "" {
			return result
		} else {
			return "Nije nadjen"
		}
	} else {
		return "Nije nadjen"
	}

}

func checkBloomFilter(file *os.File, key string) bool {
	var bF bloomfilter.BloomFilter
	var bfpos int64
	var bfDS int64
	// cita gde je bf
	file.Seek(16, 0)
	bufferedReader := bufio.NewReader(file)
	byteSlice := make([]byte, M_SIZE)
	_, err := bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfpos)

	// cita velicinu data u bf
	file.Seek(24, 0)
	bufferedReader = bufio.NewReader(file)
	byteSlice = make([]byte, M_SIZE)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfDS)

	// ide na poziciju bf i cita M
	file.Seek(bfpos, 0)
	bufferedReader = bufio.NewReader(file)
	byteSlice = make([]byte, M_SIZE)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	var bfM int64
	binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &bfM)
	bF.M = uint(bfM)

	byteSlice = make([]byte, bfDS)
	_, err = bufferedReader.Read(byteSlice)
	if err != nil {
		panic(err)
	}
	bF.Data = byteSlice

	var forRead int64
	for {
		forRead = 0
		// citam velicnu jendog podatka
		byteSlice = make([]byte, K_SIZE)
		_, err = bufferedReader.Read(byteSlice)
		if err != nil {
			break
			// panic(err)
		}
		binary.Read(bytes.NewReader(byteSlice), binary.LittleEndian, &forRead)

		// pravim prostor unapred poznate velicine i citam toliko bajtova
		byteSlice = make([]byte, forRead)
		_, err = bufferedReader.Read(byteSlice)
		if err != nil {
			break
			// panic(err)
		}
		bF.HashFunctions = append(bF.HashFunctions, bloomfilter.HashWithSeed{Seed: byteSlice})

	}
	if bF.Read([]byte(key)) {
		return true
	} else {
		return false
	}
}

func checkSummary(file *os.File, key string) string {
	file.Seek(0, 0)
	bufferedReader := bufio.NewReader(file)
	dsb := make([]byte, K_SIZE)
	isb := make([]byte, K_SIZE)
	bfb := make([]byte, K_SIZE)
	_, err := bufferedReader.Read(dsb)
	if err != nil {
		return "Greska"
	}
	_, err = bufferedReader.Read(isb)
	if err != nil {
		return "Greska"
	}
	_, err = bufferedReader.Read(bfb)
	if err != nil {
		return "Greska"
	}
	var ds int64
	var is int64
	var bf int64
	binary.Read(bytes.NewReader(dsb), binary.LittleEndian, &ds)
	binary.Read(bytes.NewReader(isb), binary.LittleEndian, &is)
	binary.Read(bytes.NewReader(bfb), binary.LittleEndian, &bf)
	sumPos := ds + is - HEADER_SIZE

	file.Seek(int64(sumPos), 0)
	bufferedReader = bufio.NewReader(file)
	var keyLen int64
	keyLenB := make([]byte, K_SIZE)
	_, err = bufferedReader.Read(keyLenB)
	if err != nil {
		return "Greska"
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(otherLenB)
	if err != nil {
		return "Greska"
	}
	key1 := string(otherLenB[0:keyLen])
	if key < key1 {
		return "Nije nadjen"
	}
	var index1 int64
	binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index1)
	sumPos += K_SIZE + keyLen + VALUE_SIZE_LEN

	var key2 string
	var index2 int64
	for sumPos < bf {
		var keyLen int64
		keyLenB := make([]byte, K_SIZE)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return "Greska"
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return "Greska"
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		if key >= key1 && key2 > key {
			return checkIndexZone(key, index1, index2, file, ds+HEADER_SIZE, is) // vratiti nes
		}
		sumPos += K_SIZE + keyLen + VALUE_SIZE_LEN
		key1 = key2
		index1 = index2
	}
	if key >= key2 {
		return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is) // vrattiti nes
	}

	return ""
}

func checkIndexZone(key string, iPos int64, maxPos int64, file *os.File, ds int64, is int64) string {
	file.Seek(int64(iPos), 0)
	bufferedReader := bufio.NewReader(file)
	var keyLen int64
	keyLenB := make([]byte, K_SIZE)
	_, err := bufferedReader.Read(keyLenB)
	if err != nil {
		return "Greska"
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(otherLenB)
	if err != nil {
		return "Greska"
	}
	key1 := string(otherLenB[0:keyLen])
	if key < key1 {
		return "Nije nadjen"
	}
	var index1 int64
	binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index1)
	iPos += K_SIZE + keyLen + VALUE_SIZE_LEN

	key2 := key1
	index2 := index1
	for iPos < maxPos {
		var keyLen int64
		keyLenB := make([]byte, K_SIZE)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return "Greska"
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return "Greska"
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		if key >= key1 && key2 > key {
			return checkDataZone(key, index1, index2, file, ds) // vratiti nes
		}
		iPos += K_SIZE + keyLen + VALUE_SIZE_LEN
		key1 = key2
		index1 = index2
	}
	if key >= key2 && maxPos == ds+is-HEADER_SIZE {
		return checkDataZone(key, index2, ds, file, ds) // vrattiti nes
	} else {
		var keyLen int64
		keyLenB := make([]byte, K_SIZE)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return "Greska"
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return "Greska"
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		return checkDataZone(key, index1, index2, file, ds)
	}
}

func checkDataZone(key string, iPos int64, maxPos int64, file *os.File, ds int64) string {
	file.Seek(int64(iPos), 0)
	var keyLen int64
	var valueLen int64
	var newKey string
	bufferedReader := bufio.NewReader(file)
	keyLenB := make([]byte, KEY_SIZE_LEN)
	_, err := bufferedReader.Read(keyLenB)
	if err != nil {
		return "Greska"
	}
	valueLenB := make([]byte, VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(valueLenB)
	if err != nil {
		return "Greska"
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	binary.Read(bytes.NewReader(valueLenB), binary.LittleEndian, &valueLen)

	otherB := make([]byte, keyLen+valueLen+TIMESTAMP_LEN+TOMBSTONE_LEN)
	_, err = bufferedReader.Read(otherB)
	if err != nil {
		return "Greska"
	}
	newKey = string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN : TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen])
	// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen]), binary.LittleEndian, &newKey)
	if key == newKey {
		// var vrednost string
		vrednost := string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:])
		// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
		return vrednost
	}
	iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
	for iPos < maxPos {
		file.Seek(iPos, 0)
		bufferedReader = bufio.NewReader(file)
		keyLenB = make([]byte, KEY_SIZE_LEN)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return "Greska"
		}
		valueLenB = make([]byte, VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(valueLenB)
		if err != nil {
			return "Greska"
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		binary.Read(bytes.NewReader(valueLenB), binary.LittleEndian, &valueLen)

		otherB = make([]byte, keyLen+valueLen+TIMESTAMP_LEN+TOMBSTONE_LEN)
		_, err = bufferedReader.Read(otherB)
		if err != nil {
			return "Greska"
		}
		newKey = string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN : TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen])
		// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen]), binary.LittleEndian, &newKey)
		if key == newKey {
			// var vrednost string
			vrednost := string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:])
			// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
			return vrednost
		}
		iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
	}
	return "Nije nadjen"
}
