package sstable

import (
	"encoding/binary"
	"key-value-engine/config"
	"key-value-engine/types"
	"os"
	"sort"
)

// Kreira SSTable: .data, .index, .summary, .filter
func CreateSSTable(path string, entries []*types.Entry) error {
	// 1. Sortiraj po ključu
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})

	dataFile, err := os.Create(path + ".data")
	if err != nil {
		return err
	}
	defer dataFile.Close()

	indexFile, err := os.Create(path + ".index")
	if err != nil {
		return err
	}
	defer indexFile.Close()

	var offset uint64 = 0
	var indexEntries [][]byte

	// 2. Inicijalizuj Bloom Filter
	bloom := NewBloomFilter(1024, 3)

	// 3. Upis u data + priprema index
	for _, entry := range entries {
		serialized := entry.Serialize()
		_, err := dataFile.Write(serialized)
		if err != nil {
			return err
		}

		// upiši key, offset u index
		keySize := uint64(len(entry.Key))
		buf := make([]byte, 8+keySize+8)
		binary.LittleEndian.PutUint64(buf[0:8], keySize)
		copy(buf[8:8+keySize], entry.Key)
		binary.LittleEndian.PutUint64(buf[8+keySize:], offset)
		indexEntries = append(indexEntries, buf)

		_, err = indexFile.Write(buf)
		if err != nil {
			return err
		}

		offset += uint64(len(serialized))

		// dodaj u bloom filter
		bloom.Add(entry.Key)
	}

	// 4. Sačuvaj Bloom Filter
	err = bloom.SaveToFile(path + ".filter")
	if err != nil {
		return err
	}

	// 5. Sačuvaj Summary
	cfg := config.LoadConfig("config.json")
	err = writeSummary(path, indexEntries, cfg.SummaryStep)
	if err != nil {
		return err
	}

	return nil
}

// Summary fajl — upisuje svaki N-ti index zapis
func writeSummary(path string, indexEntries [][]byte, step int) error {
	summaryFile, err := os.Create(path + ".summary")
	if err != nil {
		return err
	}
	defer summaryFile.Close()

	for i := 0; i < len(indexEntries); i += step {
		entry := indexEntries[i]
		_, err := summaryFile.Write(entry)
		if err != nil {
			return err
		}
	}

	return nil
}
