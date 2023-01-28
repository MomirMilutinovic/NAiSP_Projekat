package sstable

import (
	"encoding/binary"
	"os"

	wal "github.com/darkokos/NAiSP_Projekat/WAL"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

type SummaryEntry struct {
	FirstKey string
	LastKey  string
	Offset   int64
}

// Pise deo summary-a koji sadrzi granice sstabele
func writeSummaryHeader(f *os.File, first *memtable.MemTableEntry, last *memtable.MemTableEntry) {
	begin_key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	end_key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)

	binary.LittleEndian.PutUint64(begin_key_size_bytes, uint64(len(first.Key)))
	binary.LittleEndian.PutUint64(end_key_size_bytes, uint64(len(last.Key)))

	err := binary.Write(f, binary.LittleEndian, begin_key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, end_key_size_bytes)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, first.Key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(f, binary.LittleEndian, last.Key)
	if err != nil {
		panic(err)
	}

}

//Ospezi u summary-u su intervali oblika [pocetak, kraj)
func writeSummaryEntry(f *os.File, first *memtable.MemTableEntry, last *memtable.MemTableEntry, offset int64) {
	writeSummaryHeader(f, first, last)

	offset_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offset_bytes, uint64(offset))

	binary.Write(f, binary.LittleEndian, offset_bytes)
}

func readSummaryEntry(summary_file *os.File) *SummaryEntry {

	size_bytes := make([]byte, 8)

	err := binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil
	}

	first_key_size := binary.LittleEndian.Uint64(size_bytes)

	err = binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil
	}

	last_key_size := binary.LittleEndian.Uint64(size_bytes)

	//TODO: Ozbediti se od lose ucitanih velicina
	first_key := make([]byte, first_key_size)
	last_key := make([]byte, last_key_size)
	binary.Read(summary_file, binary.LittleEndian, first_key)
	binary.Read(summary_file, binary.LittleEndian, last_key)

	binary.Read(summary_file, binary.LittleEndian, size_bytes)
	offset := binary.LittleEndian.Uint64(size_bytes)

	summaryEntry := SummaryEntry{FirstKey: string(first_key), LastKey: string(last_key), Offset: int64(offset)}

	return &summaryEntry
}

// Vraca summary zapis u ciji opseg upada key ili nil ako takvog zapisa nema ili dodje do greske
func findSummaryEntry(summary_file *os.File, key []byte) *SummaryEntry {

	key_string := string(key)

	size_bytes := make([]byte, 8)

	err := binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil
	}

	first_key_size := binary.LittleEndian.Uint64(size_bytes)

	err = binary.Read(summary_file, binary.LittleEndian, size_bytes)
	if err != nil {
		return nil
	}

	last_key_size := binary.LittleEndian.Uint64(size_bytes)

	//TODO: Ozbediti se od lose ucitanih velicina
	first_key := make([]byte, first_key_size)
	last_key := make([]byte, last_key_size)
	binary.Read(summary_file, binary.LittleEndian, first_key)
	binary.Read(summary_file, binary.LittleEndian, last_key)

	if key_string < string(first_key) || key_string > string(last_key) {
		return nil
	}

	currentSummaryEntry := readSummaryEntry(summary_file)

	for currentSummaryEntry != nil {
		if currentSummaryEntry.FirstKey <= key_string && key_string <= currentSummaryEntry.LastKey {
			return currentSummaryEntry
		}

		if currentSummaryEntry.LastKey == string(last_key) {
			break
		}

		currentSummaryEntry = readSummaryEntry(summary_file)
	}

	return nil
}
