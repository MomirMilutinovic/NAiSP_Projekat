package sstable

import (
	"time"

	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
)

const (
	MAX_KEY_VAL_SIZE = 100000000 // 100 MB je maksimalna velicina kljuca ili vrednosti u bazi
)

// Format SSTable i WAL-a su isti
type SSTableEntry wal.WALEntry

// Pravi SSTable zapis od MemTable zapisa
// Prakticno receno: wrapper za racunanje CRC-a
/*func CreateSSTableEntryFromMemTableEntry(memtableEntry *memtable.MemTableEntry) *SSTableEntry {

	entry := SSTableEntry{
		CRC:       CalculateCRC(memtableEntry.Timestamp, memtableEntry.Tombstone, uint64(len(memtableEntry.Key)), uint64(len(memtableEntry.Value)), memtableEntry.Key, memtableEntry.Value),
		Timestamp: memtableEntry.Timestamp,
		KeySize:   uint64(len(memtableEntry.Key)),
		ValueSize: uint64(len(memtableEntry.Value)),
		Key:       memtableEntry.Key,
		Value:     memtableEntry.Value,
	}

	return &entry
}*/

// Funkcija pravi SSTable entry od polja koja ce biti u njemu
func CreateSSTableEntry(key []byte, value []byte, timestamp int64, tombstone bool) *SSTableEntry {

	entry := SSTableEntry{
		CRC:       CalculateCRC(timestamp, tombstone, uint64(len(key)), uint64(len(value)), key, value),
		Timestamp: timestamp,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
	}

	return &entry
}

// Funkcija pravi SSTableEntry sa kljucem key i vrednoscu value, timestamp-om sa trenutnim vremenom i tombstone-om false
func CreateFreshSSTableEntry(key []byte, value []byte) *SSTableEntry {
	timestamp := time.Now().UnixNano()
	tombstone := false
	entry := SSTableEntry{
		CRC:       CalculateCRC(timestamp, tombstone, uint64(len(key)), uint64(len(value)), key, value),
		Timestamp: timestamp,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
	}

	return &entry
}
