package sstable

import (
	"encoding/binary"
	"hash/fnv"
	"os"
)

type BloomFilter struct {
	size uint64
	bits []byte
	k    int // broj hash funkcija
}

// Novi Bloom Filter sa zadatom veličinom i brojem hash funkcija
func NewBloomFilter(size uint64, k int) *BloomFilter {
	return &BloomFilter{
		size: size,
		bits: make([]byte, size),
		k:    k,
	}
}

// Dodaj ključ u filter
func (bf *BloomFilter) Add(data []byte) {
	for i := 0; i < bf.k; i++ {
		index := bf.hash(data, uint64(i)) % (bf.size * 8)
		byteIndex := index / 8
		bitPos := index % 8
		bf.bits[byteIndex] |= 1 << bitPos
	}
}

// Proveri da li ključ možda postoji
func (bf *BloomFilter) MightContain(data []byte) bool {
	for i := 0; i < bf.k; i++ {
		index := bf.hash(data, uint64(i)) % (bf.size * 8)
		byteIndex := index / 8
		bitPos := index % 8
		if (bf.bits[byteIndex] & (1 << bitPos)) == 0 {
			return false
		}
	}
	return true
}

// Hash funkcija bazirana na FNV
func (bf *BloomFilter) hash(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	seedBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seedBytes, seed)
	h.Write(seedBytes)
	h.Write(data)
	return h.Sum64()
}

// Serijalizacija
func (bf *BloomFilter) SaveToFile(path string) error {
	return os.WriteFile(path, bf.bits, 0644)
}
