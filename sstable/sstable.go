package sstable

import (
	"encoding/binary"
	"fmt"
	"key-value-engine/config"
	"key-value-engine/types"
	"key-value-engine/wal"
	"os"
	"sort"
)

// Glavna funkcija za kreiranje SSTable fajlova
func CreateSSTable(path string, entries []*types.Entry) error {
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

	bloom := NewBloomFilter(1024, 3)

	for _, entry := range entries {
		serialized := entry.Serialize()

		_, err := dataFile.Write(serialized)
		if err != nil {
			return err
		}

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
		bloom.Add(entry.Key)
	}

	// Snimi Bloom filter
	err = bloom.SaveToFile(path + ".filter")
	if err != nil {
		return err
	}

	// Snimi Summary fajl
	cfg := config.LoadConfig("config.json")
	err = writeSummary(path, indexEntries, cfg.SummaryStep)
	if err != nil {
		return err
	}

	// Merkle stablo
	var values [][]byte
	for _, entry := range entries {
		values = append(values, entry.Value)
	}

	tree := NewMerkleTree(values)
	err = os.WriteFile(path+".meta", tree.Root.Hash, 0644)
	if err != nil {
		return err
	}

	return nil
}

// Summary fajl sadrži svaki N-ti zapis iz Index fajla
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

// Verifikacija Merkle stabla za datu SSTable instancu
func VerifySSTable(path string) error {
	f, err := os.Open(path + ".data")
	if err != nil {
		return err
	}
	defer f.Close()

	var values [][]byte
	for {
		entry, err := wal.ReadOneEntry(f)
		if err != nil {
			break
		}
		values = append(values, entry.Value)
	}

	tree := NewMerkleTree(values)
	computed := tree.Root.Hash

	expected, err := os.ReadFile(path + ".meta")
	if err != nil {
		return err
	}

	if string(computed) == string(expected) {
		fmt.Println("✅ Merkle validacija uspešna!")
	} else {
		fmt.Println("❌ Merkle NE validna! Podaci su izmenjeni.")
	}

	return nil
}
