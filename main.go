package main

import (
	"fmt"
	"key-value-engine/memtable"
	"key-value-engine/sstable"
	"key-value-engine/types"
)

func main() {
	fmt.Println("▶ Pokrećem test Memtable + SSTable")

	// 1. Inicijalizuj Memtable
	mt := memtable.NewMemtable(3)

	// 2. Dodaj zapise
	mt.Put("c", "ccc")
	mt.Put("a", "aaa")
	mt.Put("b", "bbb")

	// 3. Ispiši iz Memtable
	fmt.Println("▶ Memtable sadrži:")
	for k, v := range mt.GetAll() {
		fmt.Printf("  Key: %s, Value: %s, Tombstone: %d\n", k, v.Value, v.Tombstone)
	}

	// 4. Konvertuj u slice
	var entries []*types.Entry
	for _, v := range mt.GetAll() {
		entries = append(entries, v)
	}

	// 5. Kreiraj SSTable na disku
	err := sstable.CreateSSTable("sstable_test", entries)
	if err != nil {
		panic(err)
	}

	fmt.Println("✅ SSTable kreiran: sstable_test.data i sstable_test.index")
}
