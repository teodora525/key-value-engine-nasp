package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	MemtableMaxSize int    `json:"memtable_max_size"`
	BlockSizeKB     int    `json:"block_size_kb"`
	CacheSize       int    `json:"cache_size"`
	SummaryStep     int    `json:"summary_step"`
	MemtableType    string `json:"memtable_type"`
}

var DefaultConfig = Config{
	MemtableMaxSize: 1000,
	BlockSizeKB:     4,
	CacheSize:       128,
	SummaryStep:     5,
	MemtableType:    "hashmap",
}

// Učitavanje iz config fajla
func LoadConfig(path string) Config {
	file, err := os.Open(path)
	if err != nil {
		return DefaultConfig
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	config := DefaultConfig
	err = decoder.Decode(&config)
	if err != nil {
		return DefaultConfig
	}

	return config
}
