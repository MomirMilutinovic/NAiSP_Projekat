package sstable

import (
	"encoding/binary"
	"io"
	"os"

	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
	bloomfilter "github.com/MomirMilutinovic/NAiSP_Projekat/bloom-filter"
)

// Funkcija pokusava da procita sledeci zapis u SSTabeli na koju pokazuje fajl
// deskriptor sstableFile.
// Funkcija vraca par koji se sastoji od zapisa u SSTabeli i bool-a koji govori
// da li je citanje bilo uspesno.
// Funkcija vraca sledeci zapis i true ako uspesno procita zapis.
// Funkcija vraca nil i true ako nema vise zapisa koji treba da se procitaju.
// Funckcija vraca nil i false ako je doslo do greske u citanju fajla.
func ReadOneSSTEntry(sstableFile *os.File) (entry *SSTableEntry, ok bool) {
	// Mozda da se vracaju dva bool-a, hasNext i err
	//s := SSTableEntry

	crc_bytes := make([]byte, wal.CRC_SIZE)
	timestamp_bytes := make([]byte, wal.TIMESTAMP_SIZE)
	tombstone_byte := make([]byte, wal.TOMBSTONE_SIZE)
	key_size_bytes := make([]byte, wal.KEY_SIZE_SIZE)
	value_size_bytes := make([]byte, wal.VALUE_SIZE_SIZE)

	//TODO: Copy paste-ovanje binary.Read-a nije bas lepo
	err := binary.Read(sstableFile, binary.LittleEndian, crc_bytes)
	if err != nil {
		if err == io.EOF {
			return nil, true
		}
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, timestamp_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, tombstone_byte)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, key_size_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, value_size_bytes)
	if err != nil {
		return nil, false
	}

	crc := binary.LittleEndian.Uint32(crc_bytes)
	timestamp := binary.LittleEndian.Uint64(timestamp_bytes)

	tombstone := false
	if tombstone_byte[0] == 1 {
		tombstone = true
	}

	key_size := binary.LittleEndian.Uint64(key_size_bytes)
	value_size := binary.LittleEndian.Uint64(value_size_bytes)

	if key_size > MAX_KEY_VAL_SIZE || value_size > MAX_KEY_VAL_SIZE {
		return nil, false
	}

	key_bytes := make([]byte, key_size)
	value_bytes := make([]byte, value_size)

	err = binary.Read(sstableFile, binary.LittleEndian, key_bytes)
	if err != nil {
		return nil, false
	}

	err = binary.Read(sstableFile, binary.LittleEndian, value_bytes)
	if err != nil {
		return nil, false
	}

	entry = &SSTableEntry{
		CRC:       crc,
		Timestamp: int64(timestamp),
		Tombstone: tombstone,
		KeySize:   key_size,
		ValueSize: value_size,
		Key:       key_bytes,
		Value:     value_bytes,
	}

	if CheckSSTEntryCRC(entry) {
		return entry, true
	} else {
		os.Stderr.WriteString("CRC provera nije uspela\n")
		return nil, false
	}
}

// Funkcija trazi zapis u SSTabeli koji ima kljuc key
// Funkcija vraca nadjeni zapis ili nil ako nema tog zapisa u SSTabeli ili ako
// je doslo do greske u citanju SStabele
// Ukoliko je indexFilename prazan string, funkcija ce tretirati SSTable kao
// strukturu koja je samo u jednom fajlu.
// U suprotnom se index, summary i filter se redom citaju iz fajlova
// indexFilename, summaryFilename i filterFilename
func ReadOneSSTEntryWithKey(key []byte, sstFileName string, indexFilename string, summaryFilename string, filterFilename string) *SSTableEntry {

	key_string := string(key)

	summaryIterator := &SummaryIterator{}
	indexIterator := &IndexIterator{}
	sstableIterator := &SSTableIterator{}
	filter := &bloomfilter.BloomFilter{}

	if indexFilename != "" {
		summaryIterator = GetSummaryIteratorFromFile(summaryFilename)
		indexIterator = GetIndexIteratorFromIndexFile(indexFilename)
		sstableIterator = GetSSTableIterator(sstFileName)
		filter = ReadFilterAsSeparateFile(filterFilename)

	} else {
		summaryIterator = GetSummaryIteratorFromSSTableFile(sstFileName)
		indexIterator = GetIndexIteratorFromSSTableFile(sstFileName)
		sstableIterator = GetSSTableIterator(sstFileName)
		filter = ReadFilterFromSSTFile(sstFileName)
	}

	if summaryIterator == nil || indexIterator == nil || sstableIterator == nil || filter == nil {
		return nil
	}

	if !filter.Find(key) {
		//fmt.Println("Nije prosao filter")
		return nil
	}

	summaryEntry := summaryIterator.Seek(key_string)

	if summaryEntry == nil {
		return nil
	}

	indexIterator.SeekToOffset(summaryEntry.Offset)

	// Znamo da se kljuc sigurno ne nalazi u opsegu sledeceg summary entry-a pa
	// to mozemo koristiti za raniji prekid pretrage
	// Takodje ne moramo gledati da li je string manji od krajnjeg string
	// za dati summary zapis
	nextSummaryEntry := summaryIterator.Next()

	if nextSummaryEntry != nil {
		indexIterator.SetEndOffset(nextSummaryEntry.Offset)
	}

	indexEntry := indexIterator.SeekAndClose(key_string)

	if indexEntry == nil {
		return nil
	} else {
		sstableIterator.SeekToOffset(indexEntry.Offset)
		return sstableIterator.Next()
	}
}
