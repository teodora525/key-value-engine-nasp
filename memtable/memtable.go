package memtable

import (
	"errors"
	"key-value-engine/types"
	"sync"
	"time"
)

type Memtable struct {
	data    map[string]*types.Entry
	maxSize int
	mutex   sync.RWMutex
	size    int
}

// Kreiranje nove Memtable
func NewMemtable(maxSize int) *Memtable {
	return &Memtable{
		data:    make(map[string]*types.Entry),
		maxSize: maxSize,
	}
}

// Put - dodaje se novi unos
func (m *Memtable) Put(key, value string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.size >= m.maxSize {
		return errors.New("memtable je puna")
	}

	entry := types.NewEntry(time.Now().Unix(), 0, []byte(key), []byte(value))
	m.data[key] = entry
	m.size++
	return nil
}

// Get - dobavlja vrednost
func (m *Memtable) Get(key string) (string, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	entry, ok := m.data[key]
	if !ok || entry.Tombstone == 1 {
		return "", false
	}
	return string(entry.Value), true
}

// Delete - logicko brisanje
func (m *Memtable) Delete(key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	entry := types.NewEntry(time.Now().Unix(), 1, []byte(key), []byte{})
	m.data[key] = entry
	m.size++
	return nil
}

func (m *Memtable) GetAll() map[string]*types.Entry {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.data
}

func (m *Memtable) Size() int {
	return m.size
}
