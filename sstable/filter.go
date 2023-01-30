package sstable

import (
	"encoding/binary"
	"io"
	"os"

	bloomfilter "github.com/darkokos/NAiSP_Projekat/bloom-filter"
	"github.com/darkokos/NAiSP_Projekat/memtable"
)

const (
	FALSE_POSITIVE_RATE = 0.01
)

func writeFilter(f *os.File, entries []*memtable.MemTableEntry) {

	// TODO: Konfigurasti false-positive rate
	filter := bloomfilter.CreateBloomFilterBasedOnParams(len(entries), FALSE_POSITIVE_RATE)

	for _, entry := range entries {
		filter.Add(entry.Key)
	}

	filter_bytes := filter.Serialize()

	serialized_length := uint64(len(filter_bytes))

	binary.Write(f, binary.LittleEndian, serialized_length)
	binary.Write(f, binary.LittleEndian, filter_bytes)
}

func readFilter(f *os.File) *bloomfilter.BloomFilter {

	bloom_filter_size_bytes := make([]byte, 8)

	err := binary.Read(f, binary.LittleEndian, bloom_filter_size_bytes)
	if err != nil {
		return nil
	}

	//TODO: Osigurati da se ne procita prevelika velicina bloom filtera
	bloom_filter_size := binary.LittleEndian.Uint64(bloom_filter_size_bytes)

	bloom_filter_bytes := make([]byte, bloom_filter_size)

	err = binary.Read(f, binary.LittleEndian, bloom_filter_bytes)
	if err != nil {
		return nil
	}

	filter := bloomfilter.Deserialize(bloom_filter_bytes)
	return &filter
}

func ReadFilterAsSeparateFile(filename string) *bloomfilter.BloomFilter {
	filterFile, err := os.Open(filename)
	defer filterFile.Close()

	if err != nil {
		return nil
	}

	filter := readFilter(filterFile)

	return filter
}

func ReadFilterFromSSTFile(filename string) *bloomfilter.BloomFilter {
	sstFile, err := os.Open(filename)
	if err != nil {
		sstFile.Close()
		return nil
	}

	footer := ReadSSTFooter(sstFile)
	if footer == nil {
		sstFile.Close()
		return nil
	}

	startOfBloomFilter := footer.FilterOffset

	_, err = sstFile.Seek(startOfBloomFilter, io.SeekStart)
	if err != nil {
		sstFile.Close()
		return nil
	}

	filter := readFilter(sstFile)
	if filter == nil {
		sstFile.Close()
		return nil
	}

	return filter
}
