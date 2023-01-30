package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
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
	TokenRefreshTime       float64 `yaml:"tokenRefreshTime`
}

func NewConfig(filename string) *Config {
	var config Config
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("yamlFile.Get err   #%v ", err)
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		fmt.Printf("Unmarshal: %v", err)
	}

	fmt.Println(config.BloomExpectedElements)
	fmt.Println(config.BloomFalsePositiveRate)
	fmt.Println(config.CacheCapacity)
	fmt.Println(config.CmsEpsilon)
	fmt.Println(config.CmsDelta)
	fmt.Println(config.MemtableSize)
	fmt.Println(config.SkipListHeight)
	fmt.Println(config.TokenNumber)
	fmt.Println(config.TokenRefreshTime)

	return &config
}
