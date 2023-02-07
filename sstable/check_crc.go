package sstable

import (
	"encoding/binary"
	"hash/crc32"

	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
)

// Funkcija proverava da li CRC u prosledjenom SSTableEntry-u odgovara ostatku njegovog sadrzaja.
// Funkcija vraca true ako je CRC checksum validan, a false ako nije.
func CheckSSTEntryCRC(entry *SSTableEntry) bool {

	crc_calculated := CalculateCRC(entry.Timestamp, entry.Tombstone, entry.KeySize, entry.ValueSize, entry.Key, entry.Value)
	return crc_calculated == entry.CRC
}

// Funkcija racuna CRC za polja SSTabele/WAL-a koja su data kao parametri.
// Vraca int32 koji predstavlja izracunat CRC.
func CalculateCRC(timestamp int64, tombstone bool, keySize uint64, valueSize uint64, key []byte, value []byte) uint32 {
	crc_calculated := crc32.NewIEEE()

	b := make([]byte, wal.TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(b, uint64(timestamp))
	crc_calculated.Write(b)

	b = make([]byte, wal.TOMBSTONE_SIZE)
	if tombstone {
		b[0] = 1
	}
	crc_calculated.Write(b)

	b = make([]byte, wal.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(b, keySize)
	crc_calculated.Write(b)

	b = make([]byte, wal.VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(b, valueSize)
	crc_calculated.Write(b)

	crc_calculated.Write(key)

	crc_calculated.Write(value)

	return crc_calculated.Sum32()
}
