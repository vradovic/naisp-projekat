package simhash

import (
	"regexp"
	"strings"
)

type SimHash struct {
	text    string
	hashVal byte
}

func newSimHash(text string) *SimHash {
	reg, _ := regexp.Compile("[.,!?]")
	text = reg.ReplaceAllString(text, "")
	words := strings.Fields(text)

	countedWords := make(map[string]int)

	for i := 0; i < len(words); i++ {
		_, ok := countedWords[words[i]]
		if ok {
			countedWords[words[i]] += 1
		} else {
			countedWords[words[i]] = 1
		}
	}

}
