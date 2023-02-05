package simhash

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type SimHash struct {
	Text        string
	FingerPrint []byte
}

func NewSimHash(text string) *SimHash {
	reg, _ := regexp.Compile("[.,'!?]")
	convertedText := reg.ReplaceAllString(text, "")
	//list of words
	words := strings.Fields(convertedText)

	countedWords := make(map[string]int)
	checkMap := make(map[string]int)

	//count words that occure multiple times
	for i := 0; i < len(words); i++ {
		_, ok2 := checkMap[words[i]]
		if ok2 {
			checkMap[words[i]] += 1
		} else {
			checkMap[words[i]] = 1
		}
		key := ToBinary(GetMD5Hash(words[i]))
		_, ok := countedWords[key]
		if ok {
			countedWords[key] += 1
		} else {
			countedWords[key] = 1
		}
	}

	//adding weights to words
	summedWeights := make([]int, 256)
	for key, value := range countedWords {
		for i := 0; i < len(key); i++ {
			num, err := strconv.Atoi(string(key[i]))
			if err != nil {
				fmt.Println(err)
			}
			if num == 1 {
				summedWeights[i] += num * value
			} else {
				summedWeights[i] -= value
			}
		}
	}

	var fingerPrint []byte
	for i := 0; i < len(summedWeights); i++ {
		if summedWeights[i] > 0 {
			fingerPrint = append(fingerPrint, 1)
		} else {
			fingerPrint = append(fingerPrint, 0)
		}
	}

	s := SimHash{text, fingerPrint}
	return &s
}

func (s1 SimHash) Distance(s2 *SimHash) int {
	result := xorBytes(s1.FingerPrint, s2.FingerPrint)
	return countOnes(result)

}

func (s SimHash) Save() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(s)

	return buffer.Bytes()
}

func Load(data []byte) *SimHash {
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)

	s := &SimHash{}
	err := decoder.Decode(s)
	if err != nil {
		panic("error while decoding")
	}

	return s
}
