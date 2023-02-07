package sstable

import (
	"encoding/binary"
	"io"
	"os"

	bloomfilter "github.com/MomirMilutinovic/NAiSP_Projekat/bloom-filter"
)

const (
	FALSE_POSITIVE_RATE = 0.01
)

// Ova funkcija se ne koristi - Pisanje bloom filtera je implementirano u sst_file_writer.go,
// ali je zasnovano na ovoj funkciji.
// Pise bloom filter u fajl tamo gde je postavljen fajl deskriptor f.
// Kao parametar prima memtable entry-e koje treba upisati.
/*
func writeFilter(f *os.File, entries []*memtable.MemTableEntry) {

	filter := bloomfilter.CreateBloomFilterBasedOnParams(len(entries), FALSE_POSITIVE_RATE)

	for _, entry := range entries {
		filter.Add(entry.Key)
	}

	filter_bytes := filter.Serialize()

	serialized_length := uint64(len(filter_bytes))

	binary.Write(f, binary.LittleEndian, serialized_length)
	binary.Write(f, binary.LittleEndian, filter_bytes)
}
*/
// Cita bloom filter koji pocinje na poziciji gde je postavljen fajl
// deskriptor f.
func readFilter(f *os.File) *bloomfilter.BloomFilter {

	bloom_filter_size_bytes := make([]byte, 8)

	err := binary.Read(f, binary.LittleEndian, bloom_filter_size_bytes)
	if err != nil {
		return nil
	}

	bloom_filter_size := binary.LittleEndian.Uint64(bloom_filter_size_bytes)

	if bloom_filter_size > MAX_KEY_VAL_SIZE {
		return nil
	}

	bloom_filter_bytes := make([]byte, bloom_filter_size)

	err = binary.Read(f, binary.LittleEndian, bloom_filter_bytes)
	if err != nil {
		return nil
	}

	filter := bloomfilter.Deserialize(bloom_filter_bytes)
	return &filter
}

// Cita bloom filter koji se nalazi u zasebnom fajlu.
// Vraca bloom filter koji je procitan ili nil ako je doslo do greske.
func ReadFilterAsSeparateFile(filename string) *bloomfilter.BloomFilter {
	filterFile, err := os.Open(filename)
	defer filterFile.Close()

	if err != nil {
		return nil
	}

	filter := readFilter(filterFile)

	return filter
}

// Nalazi bloom filter u SSTabeli koja je zapisana kao jedan fajl i cita ga.
// Vraca bloom filter koji je procitan ili nil ako je doslo do greske.
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
