package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"key-value-engine/config"
	"key-value-engine/memtable"
	"key-value-engine/sstable"
	"key-value-engine/types"
	"key-value-engine/wal"
	"os"
	"strings"
	"time"
)

func main() {
	cfg := config.LoadConfig("config.json")

	mem := memtable.NewMemtable(cfg.MemtableMaxSize)
	wlog, _ := wal.NewWAL("wal.log")
	defer wlog.Close()

	fmt.Println("===== Key-Value engine =====")
	fmt.Println("Komande: PUT <key> <value> | GET <key> | DELETE <key> | FLUSH | VERIFY | EXIT")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		line := scanner.Text()
		args := strings.Fields(line)

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PUT":
			if len(args) < 3 {
				fmt.Println(">> PUT zahteva ključ i vrednost.")
				continue
			}
			key, value := args[1], strings.Join(args[2:], " ")
			entry := types.NewEntry(time.Now().Unix(), 0, []byte(key), []byte(value))

			err := mem.Put(key, value)
			if err != nil {
				if err.Error() == "memtable je puna" {
					fmt.Println(">> Memtable pun! Kreiram SSTable...")

					entries := make([]*types.Entry, 0)
					for _, e := range mem.GetAll() {
						entries = append(entries, e)
					}
					_ = sstable.CreateSSTable("sstable_test", entries)
					printSSTableIndex("sstable_test")

					mem = memtable.NewMemtable(cfg.MemtableMaxSize)
					_ = mem.Put(key, value)
					_ = wlog.Write(entry)
					fmt.Println(">> PUT posle flush-a")
				} else {
					fmt.Println(">> Greska: ", err)
				}
				continue
			}

			wlog.Write(entry)
			fmt.Println(">> PUT uspešan")
			printMemtable(mem)

		case "GET":
			if len(args) != 2 {
				fmt.Println(">> GET zahteva tačno jedan ključ.")
				continue
			}
			key := args[1]
			value, ok := mem.Get(key)
			if ok {
				fmt.Println("Trazenje.. ", value)
			} else {
				fmt.Println(">> Nema vrednosti za dati ključ.")
			}

		case "DELETE":
			if len(args) != 2 {
				fmt.Println(">> DELETE zahteva tačno jedan ključ.")
				continue
			}
			key := args[1]
			entry := types.NewEntry(time.Now().Unix(), 1, []byte(key), []byte{})
			mem.Delete(key)
			wlog.Write(entry)
			fmt.Println(">> Obrisano")
			printMemtable(mem)

			if mem.Size() >= cfg.MemtableMaxSize {
				fmt.Println(">> Memtable pun! Kreira se SSTable...")

				entries := make([]*types.Entry, 0)
				for _, e := range mem.GetAll() {
					entries = append(entries, e)
				}
				_ = sstable.CreateSSTable("sstable_test", entries)
				printSSTableIndex("sstable_test")

				mem = memtable.NewMemtable(cfg.MemtableMaxSize)
			}

		case "FLUSH":
			entries := make([]*types.Entry, 0)
			for _, e := range mem.GetAll() {
				entries = append(entries, e)
			}
			err := sstable.CreateSSTable("sstable_test", entries)
			if err != nil {
				fmt.Println(">> Greška prilikom flushovanja:", err)
			} else {
				fmt.Println(">>  SSTable uspešno kreiran (sstable_test.*)")
				printSSTableIndex("sstable_test")
			}

		case "VERIFY":
			err := sstable.VerifySSTable("sstable_test")
			if err != nil {
				fmt.Println(">> Verifikacija nije uspela:", err)
			}

		case "EXIT":
			return

		default:
			fmt.Println(">> Nepoznata komanda.")
		}
	}
}

func printMemtable(mem *memtable.Memtable) {
	fmt.Println("===== Sadržaj Memtable-a: ======")
	for k, v := range mem.GetAll() {
		fmt.Printf("  %s = %s (Tombstone: %d)\n", k, v.Value, v.Tombstone)
	}
}

func printSSTableIndex(path string) {
	fmt.Println("====== Sadržaj SSTable Index fajla: ======")
	file, err := os.Open(path + ".index")
	if err != nil {
		fmt.Println("Greska prilikom otvaranja index fajla:", err)
		return
	}
	defer file.Close()

	for {
		// keySize(8) + key + offset(8)
		sizeBuf := make([]byte, 8)
		_, err := file.Read(sizeBuf)
		if err != nil {
			break
		}
		keySize := binary.LittleEndian.Uint64(sizeBuf)

		key := make([]byte, keySize)
		file.Read(key)

		offsetBuf := make([]byte, 8)
		file.Read(offsetBuf)
		offset := binary.LittleEndian.Uint64(offsetBuf)

		fmt.Printf("  %s @ offset %d\n", string(key), offset)
	}
}
