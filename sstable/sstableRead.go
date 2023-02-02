package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"

	"github.com/vradovic/naisp-projekat/bloomfilter"
	"github.com/vradovic/naisp-projekat/record"
)

func FindByKey(keys []string, path string, full bool) []record.Record {
	f, err := os.OpenFile(path, os.O_RDONLY, 0600) // unece se jos jedan parametar strukture SSTable za ime fajla
	if err != nil {
		panic(err)
	}
	var key string
	var keySec string
	if len(keys) == 1 {
		key = keys[0]
		keySec = ""
	} else {
		key = keys[0]
		keySec = keys[1]
	}

	maybeInFile := true
	if full && len(keys) == 1 {
		maybeInFile = checkBloomFilter(f, key)
	}
	defer f.Close()

	if maybeInFile {
		// idi u summary
		result := checkSummary(f, key, full, keySec)
		if len(result) != 0 {
			return result
		} else {
			return []record.Record{}
		}
	} else {
		return []record.Record{}
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

func checkSummary(file *os.File, key string, full bool, keySec string) []record.Record {
	file.Seek(0, 0)
	bufferedReader := bufio.NewReader(file)
	dsb := make([]byte, K_SIZE)
	isb := make([]byte, K_SIZE)
	bfb := make([]byte, K_SIZE)
	_, err := bufferedReader.Read(dsb)
	if err != nil {
		return []record.Record{}
	}
	_, err = bufferedReader.Read(isb)
	if err != nil {
		return []record.Record{}
	}
	_, err = bufferedReader.Read(bfb)
	if err != nil {
		return []record.Record{}
	}
	var ds int64
	var is int64
	var bf int64
	// vrednosti := [][]byte{}
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
		return []record.Record{}
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(otherLenB)
	if err != nil {
		return []record.Record{}
	}
	key1 := string(otherLenB[0:keyLen])
	if key < key1 {
		return []record.Record{}
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
			return []record.Record{}
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return []record.Record{}
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		if full {
			if key >= key1 && key2 > key {
				return checkIndexZone(key, index1, index2, file, ds+HEADER_SIZE, is, full, keySec) // vratiti nes
			}
		} else {
			if key >= key1[:len(key)] && key2[:len(key)] > key {
				return checkIndexZone(key, index1, index2, file, ds+HEADER_SIZE, is, full, keySec) // vratiti nes
			}
		}
		sumPos += K_SIZE + keyLen + VALUE_SIZE_LEN
		key1 = key2
		index1 = index2
	}
	if full {
		if key >= key2 {
			return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
		}
	} else {
		if key >= key2[:len(key)] {
			return checkIndexZone(key, index2, ds+is-HEADER_SIZE, file, ds, is, full, keySec) // vrattiti nes
		}
	}

	return []record.Record{}
}

func checkIndexZone(key string, iPos int64, maxPos int64, file *os.File, ds int64, is int64, full bool, keySec string) []record.Record {
	file.Seek(int64(iPos), 0)
	bufferedReader := bufio.NewReader(file)
	var keyLen int64
	keyLenB := make([]byte, K_SIZE)
	_, err := bufferedReader.Read(keyLenB)
	if err != nil {
		return []record.Record{}
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(otherLenB)
	if err != nil {
		return []record.Record{}
	}
	key1 := string(otherLenB[0:keyLen])
	if key < key1 {
		return []record.Record{}
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
			return []record.Record{}
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(otherLenB)
		if err != nil {
			return []record.Record{}
		}
		key2 = string(otherLenB[0:keyLen])
		binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
		if full {
			if key >= key1 && key2 > key {
				return checkDataZone(key, index1, index2, file, ds, full, keySec) // vratiti nes
			}
		} else {
			if key >= key1[:len(key)] && key2[:len(key)] > key {
				return checkDataZone(key, index1, index2, file, ds, full, keySec) // vratiti nes
			}
		}

		iPos += K_SIZE + keyLen + VALUE_SIZE_LEN
		key1 = key2
		index1 = index2
	}
	if full {
		if key >= key2 && maxPos == ds+is-HEADER_SIZE {
			return checkDataZone(key, index2, ds, file, ds, full, keySec) // vrattiti nes
		} else {
			var keyLen int64
			keyLenB := make([]byte, K_SIZE)
			_, err = bufferedReader.Read(keyLenB)
			if err != nil {
				return []record.Record{}
			}
			binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
			otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
			_, err = bufferedReader.Read(otherLenB)
			if err != nil {
				return []record.Record{}
			}
			key2 = string(otherLenB[0:keyLen])
			binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
			return checkDataZone(key, index1, index2, file, ds, full, keySec)
		}
	} else {
		if key >= key2[:len(key)] && maxPos == ds+is-HEADER_SIZE {
			return checkDataZone(key, index2, ds, file, ds, full, keySec) // vrattiti nes
		} else {
			var keyLen int64
			keyLenB := make([]byte, K_SIZE)
			_, err = bufferedReader.Read(keyLenB)
			if err != nil {
				return []record.Record{}
			}
			binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
			otherLenB := make([]byte, keyLen+VALUE_SIZE_LEN)
			_, err = bufferedReader.Read(otherLenB)
			if err != nil {
				return []record.Record{}
			}
			key2 = string(otherLenB[0:keyLen])
			binary.Read(bytes.NewReader(otherLenB[keyLen:]), binary.LittleEndian, &index2)
			return checkDataZone(key, index1, index2, file, ds, full, keySec)
		}
	}
}

func checkDataZone(key string, iPos int64, maxPos int64, file *os.File, ds int64, full bool, keySec string) []record.Record {
	file.Seek(int64(iPos), 0)
	var keyLen int64
	var valueLen int64
	var newKey string
	var timestamp int64
	var tombstone uint64
	var vrednosti []record.Record
	bufferedReader := bufio.NewReader(file)
	keyLenB := make([]byte, KEY_SIZE_LEN)
	_, err := bufferedReader.Read(keyLenB)
	if err != nil {
		return []record.Record{}
	}
	valueLenB := make([]byte, VALUE_SIZE_LEN)
	_, err = bufferedReader.Read(valueLenB)
	if err != nil {
		return []record.Record{}
	}
	binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
	binary.Read(bytes.NewReader(valueLenB), binary.LittleEndian, &valueLen)

	otherB := make([]byte, keyLen+valueLen+TIMESTAMP_LEN+TOMBSTONE_LEN)
	_, err = bufferedReader.Read(otherB)
	if err != nil {
		return []record.Record{}
	}
	newKey = string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN : TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen])
	// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen]), binary.LittleEndian, &newKey)
	if full {
		if key == newKey && keySec == "" {
			// var vrednost string
			vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
			var tombstoneb bool
			if tombstone == 0 {
				tombstoneb = false
			} else {
				tombstoneb = true
			}
			vrednosti = append(vrednosti, record.Record{Key: string(newKey), Value: vrednost, Timestamp: timestamp, Tombstone: tombstoneb})
			return vrednosti
		}
		if keySec != "" && key <= newKey && key <= keySec {
			vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
			var tombstoneb bool
			if tombstone == 0 {
				tombstoneb = false
			} else {
				tombstoneb = true
			}
			vrednosti = append(vrednosti, record.Record{Key: string(newKey), Value: vrednost, Timestamp: timestamp, Tombstone: tombstoneb})
		}
	} else {
		if key == newKey[:len(key)] {
			// var vrednost string
			vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
			// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
			binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
			binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
			var tombstoneb bool
			if tombstone == 0 {
				tombstoneb = false
			} else {
				tombstoneb = true
			}
			vrednosti = append(vrednosti, record.Record{Key: string(newKey), Value: vrednost, Timestamp: timestamp, Tombstone: tombstoneb})
		}
	}
	iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
	for iPos < maxPos {
		file.Seek(iPos, 0)
		bufferedReader = bufio.NewReader(file)
		keyLenB = make([]byte, KEY_SIZE_LEN)
		_, err = bufferedReader.Read(keyLenB)
		if err != nil {
			return []record.Record{}
		}
		valueLenB = make([]byte, VALUE_SIZE_LEN)
		_, err = bufferedReader.Read(valueLenB)
		if err != nil {
			return []record.Record{}
		}
		binary.Read(bytes.NewReader(keyLenB), binary.LittleEndian, &keyLen)
		binary.Read(bytes.NewReader(valueLenB), binary.LittleEndian, &valueLen)

		otherB = make([]byte, keyLen+valueLen+TIMESTAMP_LEN+TOMBSTONE_LEN)
		_, err = bufferedReader.Read(otherB)
		if err != nil {
			return []record.Record{}
		}
		newKey = string(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN : TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen])
		if newKey > keySec || iPos > ds {
			return vrednosti
		}
		// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen]), binary.LittleEndian, &newKey)
		if full {
			if key == newKey && keySec == "" {
				// var vrednost string
				vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
				var tombstoneb bool
				if tombstone == 0 {
					tombstoneb = false
				} else {
					tombstoneb = true
				}
				vrednosti = append(vrednosti, record.Record{Key: string(newKey), Value: vrednost, Timestamp: timestamp, Tombstone: tombstoneb})
				return vrednosti
			}
			if keySec != "" && key <= newKey && key <= keySec {
				vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
				var tombstoneb bool
				if tombstone == 0 {
					tombstoneb = false
				} else {
					tombstoneb = true
				}
				vrednosti = append(vrednosti, record.Record{Key: string(newKey), Value: vrednost, Timestamp: timestamp, Tombstone: tombstoneb})
			}

		} else {
			if key == newKey[:len(key)] {
				// var vrednost string
				vrednost := otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]
				// binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN+TOMBSTONE_LEN+keyLen:]), binary.LittleEndian, &vrednost)
				binary.Read(bytes.NewReader(otherB[:TIMESTAMP_LEN]), binary.LittleEndian, &timestamp)
				binary.Read(bytes.NewReader(otherB[TIMESTAMP_LEN:TIMESTAMP_LEN+TOMBSTONE_LEN]), binary.LittleEndian, &tombstone)
				var tombstoneb bool
				if tombstone == 0 {
					tombstoneb = false
				} else {
					tombstoneb = true
				}
				vrednosti = append(vrednosti, record.Record{Key: string(newKey), Value: vrednost, Timestamp: timestamp, Tombstone: tombstoneb})
			}
		}
		if keySec != "" {
			maxPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
		}
		iPos += keyLen + valueLen + KEY_SIZE_LEN + VALUE_SIZE_LEN + TIMESTAMP_LEN + TOMBSTONE_LEN
	}
	return vrednosti
}
