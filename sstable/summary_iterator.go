package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/MomirMilutinovic/NAiSP_Projekat/utils"
)

//Format summary-a:
// Offset/Pokazivac ka footer-u (8B)
// [Velicina pocetnog kljuca (8B) | Velicina krajnjeg kljuca (8B) | Pocetni kljuc (?B) | Krajnji kljuc (?B) | Offset u indeksu (8B)] x N
// Footer: [Velicina prvog kljuca (8B) | Velicina poslednjeg kljuca (8B) | Prvi kljuc (?B) | Poslednji kljuc (?B)]

// Ova struktura sluzi omogucava citanje Summary-a zapis po zapis sa diska
type SummaryIterator struct {
	summaryFile *os.File
	end_offset  int64
	Begin_key   string
	End_key     string
	Valid       bool
	Ok          bool
}

// Funkcija dobavlja granice summary-a
// Vraca redom prvi kljuc data dela, poslednji kljuc data dela, poziciju na
// kojoj se nalazi footer i bool vrednost koja govori da li je doslo do greske.
// Pozicija gde se nalazi footer predstavlja kraj summary-a.
func getBeginEndKeysAndFooterOffset(summary_file *os.File) (begin_key []byte, end_key []byte, footer_offset int64, ok bool) {
	bytes_read := make([]byte, 8)

	err := binary.Read(summary_file, binary.LittleEndian, bytes_read)
	if err != nil {
		return nil, nil, -1, false
	}

	footerOffset := int64(binary.LittleEndian.Uint64(bytes_read))

	_, err = summary_file.Seek(footerOffset, io.SeekStart)
	if err != nil {
		return nil, nil, -1, false
	}

	err = binary.Read(summary_file, binary.LittleEndian, bytes_read)
	if err != nil {
		return nil, nil, -1, false
	}

	first_key_size := binary.LittleEndian.Uint64(bytes_read)

	err = binary.Read(summary_file, binary.LittleEndian, bytes_read)
	if err != nil {
		return nil, nil, -1, false
	}

	last_key_size := binary.LittleEndian.Uint64(bytes_read)

	if first_key_size > MAX_KEY_VAL_SIZE || last_key_size > MAX_KEY_VAL_SIZE {
		return nil, nil, -1, false
	}

	first_key := make([]byte, first_key_size)
	last_key := make([]byte, last_key_size)
	binary.Read(summary_file, binary.LittleEndian, first_key)
	binary.Read(summary_file, binary.LittleEndian, last_key)

	return first_key, last_key, footerOffset, true
}

// Konstruise SummaryIterator za Summary fajl SSTabele koja je zapisana u vise fajlova.
// Vraca konstruisani iterator.
// Vraca nil ako je doslo do greske.
func GetSummaryIteratorFromFile(filename string) *SummaryIterator {
	summary_file, err := os.Open(filename)

	if err != nil {
		return nil
	}

	size := utils.GetFileSize(filename)

	if size == -1 {
		return nil
	}

	first_key, last_key, footerOffset, ok := getBeginEndKeysAndFooterOffset(summary_file)

	if !ok {
		return nil
	}

	// Vracamo se na poziciju prvog elementa summary-a (prvih 8B su pokazivac ka offsetu)
	_, err = summary_file.Seek(8, io.SeekStart) // TODO: Eliminisati ovaj magicni broj
	if err != nil {
		return nil
	}

	iter := SummaryIterator{summaryFile: summary_file, end_offset: footerOffset, Begin_key: string(first_key), End_key: string(last_key), Valid: true, Ok: true}

	return &iter
}

// Konstruise SummaryIterator za SSTabelu koju je zapisana kao jedan fajl.
// Vraca konstruisani iterator.
// Vraca nil ako je doslo do greske.
func GetSummaryIteratorFromSSTableFile(filename string) *SummaryIterator {

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

	startOfSummary := footer.SummaryOffset
	_, err = sstFile.Seek(startOfSummary, io.SeekStart)
	if err != nil {
		sstFile.Close()
		return nil
	}

	first_key, last_key, footerOffset, ok := getBeginEndKeysAndFooterOffset(sstFile)
	if !ok {
		return nil
	}

	endOfSummary := footerOffset

	_, err = sstFile.Seek(startOfSummary+8, io.SeekStart)
	if err != nil {
		sstFile.Close()
		return nil
	}

	iter := &SummaryIterator{summaryFile: sstFile, end_offset: endOfSummary, Begin_key: string(first_key), End_key: string(last_key), Valid: true, Ok: true}
	return iter
}

// Dobavlja sledeci summary zapis
// Ako se dodje do kraja summary-a ili se desi greska vraca nil i invalidara iterator i zatvara fajl
func (iter *SummaryIterator) Next() *SummaryEntry {
	cur_pos, _ := iter.summaryFile.Seek(0, io.SeekCurrent)

	if !iter.Valid {
		return nil
	}

	if cur_pos >= iter.end_offset {
		iter.Valid = false
		iter.Ok = true
		iter.summaryFile.Close()
		return nil
	}

	entry, ok := readSummaryEntry(iter.summaryFile)

	iter.Valid = (entry != nil)
	iter.Ok = ok

	if !iter.Valid {
		iter.summaryFile.Close()
	}

	return entry
}

// Nalazi zapis u summary-u u koji updada kljuc string.
// Iterator ostaje validan u ovom slucaju i bice spreman za citanje zapisa nakon vracenog.
// Ili vraca nil, invalidira iterator i zatvara fajl
func (iter *SummaryIterator) Seek(key string) *SummaryEntry {

	if key < iter.Begin_key || key > iter.End_key {
		iter.Valid = false
		iter.summaryFile.Close()
		return nil
	}

	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		if key <= entry.LastKey && entry.FirstKey <= key {
			return entry
		}
	}

	iter.summaryFile.Close()
	return nil
}

// Zatvara fajl i invalidira iterator
func (iter *SummaryIterator) Close() {
	iter.summaryFile.Close()
	iter.Valid = false
}
