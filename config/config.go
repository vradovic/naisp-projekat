package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

const (
	EXPECTED_EL         = 1000
	FALSE_POSITIVE_RATE = 0.001
	CMS_EPSILON         = 0.001
	CMS_DELTA           = 0.001
	CACHE_CAP           = 100
	MEMTABLE_SIZE       = 200
	SKIP_LIST_HEIGHT    = 10
	TOKEN_NUMBER        = 20
	TOKEN_REFRESH_TIME  = 2
)

type Config struct {
	BloomExpectedElements  int     `yaml:"bloomExpectedElements"`
	BloomFalsePositiveRate float64 `yaml:"bloomFalsePositive"`
	CacheCapacity          int     `yaml:"cacheCapacity"`
	CmsEpsilon             float64 `yaml:"cmsEpsilon"`
	CmsDelta               float64 `yaml:"cmsDelta"`
	MemtableSize           uint    `yaml:"memtableSize"`
	SkipListHeight         int     `yaml:"skipListHeight"`
	TokenNumber            int     `yaml:"tokenNumber"`
	TokenRefreshTime       float64 `yaml:"tokenRefreshTime"`
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
		config.SkipListHeight = SKIP_LIST_HEIGHT
		config.TokenNumber = TOKEN_NUMBER
		config.TokenRefreshTime = TOKEN_REFRESH_TIME

	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		fmt.Printf("Unmarshal: %v", err)
	}

	return &config

}
