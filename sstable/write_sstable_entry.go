package sstable

import (
	"encoding/binary"
	"os"

	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
)

// Funkcija pise red SSTable-a koji cuva prosledjeni
// podatak
func writeSSTableEntry(sstableFile *os.File, entry *SSTableEntry) {
	timestamp := entry.Timestamp
	timestamp_bytes := make([]byte, wal.TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestamp_bytes, uint64(timestamp))

	tombstone_byte := make([]byte, wal.TOMBSTONE_SIZE)
	if entry.Tombstone {
		tombstone_byte[0] = 1
	}

	keySize := entry.KeySize
	key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(key_size_bytes, keySize)

	valueSize := entry.ValueSize
	value_size_bytes := make([]byte, wal.VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(value_size_bytes, valueSize)

	err := binary.Write(sstableFile, binary.LittleEndian, entry.CRC)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, timestamp_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, tombstone_byte)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, value_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, entry.Key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(sstableFile, binary.LittleEndian, entry.Value)
	if err != nil {
		panic(err)
	}
}
