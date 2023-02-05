package io

import (
	"bytes"
	"encoding/gob"
	"github.com/vradovic/naisp-projekat/bloomfilter"
	"github.com/vradovic/naisp-projekat/cms"
	"github.com/vradovic/naisp-projekat/config"
	"github.com/vradovic/naisp-projekat/hll"
	"github.com/vradovic/naisp-projekat/simhash"
)

func isSpecialKey(key string) bool {
	return key[0] == '!' || key[0] == '?' || key[0] == '#' || key[0] == '%'
}

func serializeStructure(key, val string) ([]byte, error) {
	var err error
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	switch key[0] {
	case '!':
		hl := hll.NewHyperLogLog(16)
		err = encoder.Encode(hl)
	case '?':
		cmsketch := cms.NewCms(config.GlobalConfig.CmsEpsilon, config.GlobalConfig.CmsDelta)
		err = encoder.Encode(*cmsketch)
	case '#':
		simh := simhash.NewSimHash(val)
		err = encoder.Encode(*simh)
	case '%':
		bf := bloomfilter.NewBloomFilter(config.GlobalConfig.BloomExpectedElements, config.GlobalConfig.BloomFalsePositiveRate)
		err = encoder.Encode(*bf)
	}

	return buf.Bytes(), err
}
