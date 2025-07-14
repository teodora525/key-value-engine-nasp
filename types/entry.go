package types

import (
	"encoding/binary"
	"hash/crc32"
)

type Entry struct {
	CRC       uint32 // za proveru integriteta
	Timestamp int64  // kada je zapis kreiran
	Tombstone byte   // 0 za običan zapis, 1 za DELETE
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

// Kreiraj novi Entry
func NewEntry(timestamp int64, tombstone byte, key, value []byte) *Entry {
	return &Entry{
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
}

// Serialize - serijalizuje Entry u bajtove
func (e *Entry) Serialize() []byte {
	key := e.Key
	value := e.Value

	buf := make([]byte, 8+1+8+8+len(key)+len(value)) // ts + tomb + keysize + valsize + key + value

	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Timestamp))
	buf[8] = e.Tombstone
	binary.LittleEndian.PutUint64(buf[9:17], e.KeySize)
	binary.LittleEndian.PutUint64(buf[17:25], e.ValueSize)
	copy(buf[25:25+len(key)], key)
	copy(buf[25+len(key):], value)

	crc := crc32.ChecksumIEEE(buf)
	total := make([]byte, 4+len(buf))
	binary.LittleEndian.PutUint32(total[0:4], crc)
	copy(total[4:], buf)

	return total
}
