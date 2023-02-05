package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

var GlobalConfig Config

const (
	EXPECTED_EL         = 1000
	FALSE_POSITIVE_RATE = 0.001
	CMS_EPSILON         = 0.001
	CMS_DELTA           = 0.001
	CACHE_CAP           = 100
	MEMTABLE_SIZE       = 10
	STRUCTURE_TYPE      = "skiplist"
	SKIP_LIST_HEIGHT    = 10
	B_TREE_ORDER        = 3
	TOKEN_NUMBER        = 20
	TOKEN_REFRESH_TIME  = 2
	WAL_PATH            = "resources\\wal.log"
	MAX_ENTRY_SIZE      = 1024
	CRC_SIZE            = 4
	TIMESTAMP_SIZE      = 8
	TOMBSTONE_SIZE      = 1
	KEY_SIZE_SIZE       = 8
	VALUE_SIZE_SIZE     = 8
	CRC_START           = 0
	TIMESTAMP_START     = CRC_START + CRC_SIZE
	TOMBSTONE_START     = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START      = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START    = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START           = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type Config struct {
	BloomExpectedElements  int     `yaml:"bloomExpectedElements"`
	BloomFalsePositiveRate float64 `yaml:"bloomFalsePositive"`
	CacheCapacity          int     `yaml:"cacheCapacity"`
	CmsEpsilon             float64 `yaml:"cmsEpsilon"`
	CmsDelta               float64 `yaml:"cmsDelta"`
	MemtableSize           uint    `yaml:"memtableSize"`
	StructureType          string  `yaml:"structureType"`
	SkipListHeight         int     `yaml:"skipListHeight"`
	TokenNumber            int     `yaml:"tokenNumber"`
	TokenRefreshTime       float64 `yaml:"tokenRefreshTime"`
	WalPath                string  `yaml:"walPath"`
	MaxEntrySize           int     `yaml:"maxEntrySize"`
	CrcSize                int     `yaml:"crcSize"`
	TimestampSize          int     `yaml:"timestampSize"`
	TombstoneSize          int     `yaml:"tombstoneSize"`
	KeySizeSize            int     `yaml:"keySizeSize"`
	ValueSizeSize          int     `yaml:"valueSizeSize"`
	CrcStart               int     `yaml:"crcStart"`
	TimestampStart         int     `yaml:"timestampStart"`
	TombstoneStart         int     `yaml:"tombstoneStart"`
	KeySizeStart           int     `yaml:"keySizeStart"`
	ValueSizeStart         int     `yaml:"ValueSizeStart"`
	KeyStart               int     `yaml:"keyStart"`
	BTreeOrder             int     `yaml:"bTreeOrder"`
}

func NewConfig(filename string) *Config {
	var config Config
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		config.BloomExpectedElements = EXPECTED_EL
		config.BloomFalsePositiveRate = FALSE_POSITIVE_RATE
		config.CacheCapacity = CACHE_CAP
		config.CmsDelta = CMS_DELTA
		config.CmsEpsilon = CMS_EPSILON
		config.MemtableSize = MEMTABLE_SIZE
		config.StructureType = STRUCTURE_TYPE
		config.SkipListHeight = SKIP_LIST_HEIGHT
		config.TokenNumber = TOKEN_NUMBER
		config.TokenRefreshTime = TOKEN_REFRESH_TIME
		config.WalPath = WAL_PATH
		config.MaxEntrySize = MAX_ENTRY_SIZE
		config.CrcSize = CRC_SIZE
		config.TimestampSize = TIMESTAMP_SIZE
		config.TombstoneSize = TOMBSTONE_SIZE
		config.KeySizeSize = KEY_SIZE_SIZE
		config.ValueSizeSize = VALUE_SIZE_SIZE
		config.CrcStart = CRC_START
		config.BTreeOrder = B_TREE_ORDER
		config.TimestampStart = TIMESTAMP_START
		config.TombstoneStart = TOMBSTONE_START
		config.KeySizeStart = KEY_SIZE_START
		config.ValueSizeStart = VALUE_SIZE_START
		config.KeyStart = KEY_START
	} else {
		err = yaml.Unmarshal(yamlFile, &config)
		if err != nil {
			fmt.Printf("Unmarshal: %v", err)
		}
	}

	return &config
}

func Init() {
	PATH := "config\\config.yml"

	GlobalConfig = *NewConfig(PATH)

	if _, err := os.Stat(PATH); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(PATH)
		defer f.Close()
		if err != nil {
			panic(err)
		}

		out, err := yaml.Marshal(GlobalConfig)
		if err != nil {
			panic(err)
		}

		f.Write(out)
	}
}
