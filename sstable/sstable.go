package sstable

import (
	"encoding/binary"
	"key-value-engine/types"
	"os"
	"sort"
)

type SSTable struct {
	DataFile  *os.File
	IndexFile *os.File
}

func CreateSSTable(path string, entries []*types.Entry) error {
	// sortiranje po kljucu
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

	for _, entry := range entries {
		serialized := entry.Serialize()
		// upiši u data fajl
		_, err := dataFile.Write(serialized)
		if err != nil {
			return err
		}

		// upis u index fajl: keySize | key | offset
		keySize := uint64(len(entry.Key))
		buf := make([]byte, 8+keySize+8)
		binary.LittleEndian.PutUint64(buf[0:8], keySize)
		copy(buf[8:8+keySize], entry.Key)
		binary.LittleEndian.PutUint64(buf[8+keySize:], offset)

		_, err = indexFile.Write(buf)
		if err != nil {
			return err
		}

		offset += uint64(len(serialized))
	}

	return nil
}
