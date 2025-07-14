package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"key-value-engine/types"
	"os"
)

type WAL struct {
	file *os.File
}

// Kreira novi WAL ili otvara postojeći
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

// Write - upisuje novi WAL zapis
func (w *WAL) Write(entry *types.Entry) error {
	data := entry.Serialize()
	_, err := w.file.Write(data)
	return err
}

// ReadAll - cita sve zapise i proverava CRC
func (w *WAL) ReadAll() ([]*types.Entry, error) {
	var entries []*types.Entry
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	for {
		crcBuf := make([]byte, 4)
		_, err := w.file.Read(crcBuf)
		if err != nil {
			break // EOF
		}

		crc := binary.LittleEndian.Uint32(crcBuf)

		header := make([]byte, 25) // ts(8) + tomb(1) + keySize(8) + valSize(8)
		_, err = w.file.Read(header)
		if err != nil {
			return nil, errors.New("greška pri čitanju headera")
		}

		timestamp := int64(binary.LittleEndian.Uint64(header[0:8]))
		tombstone := header[8]
		keySize := binary.LittleEndian.Uint64(header[9:17])
		valueSize := binary.LittleEndian.Uint64(header[17:25])

		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		_, err = w.file.Read(key)
		if err != nil {
			return nil, errors.New("greška pri čitanju ključa")
		}
		_, err = w.file.Read(value)
		if err != nil {
			return nil, errors.New("greška pri čitanju vrednosti")
		}

		// Provera CRC
		all := make([]byte, 8+1+8+8+len(key)+len(value))
		copy(all[0:8], header[0:8])
		all[8] = tombstone
		copy(all[9:], header[9:])
		copy(all[25:], key)
		copy(all[25+len(key):], value)
		computedCRC := crc32.ChecksumIEEE(all)

		if crc != computedCRC {
			fmt.Println("CRC mismatch! Preskačem zapis.")
			continue
		}

		entry := &types.Entry{
			CRC:       crc,
			Timestamp: timestamp,
			Tombstone: tombstone,
			KeySize:   keySize,
			ValueSize: valueSize,
			Key:       key,
			Value:     value,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (w *WAL) Close() {
	w.file.Close()
}
