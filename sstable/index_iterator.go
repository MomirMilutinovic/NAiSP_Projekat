package sstable

import (
	"fmt"
	"io"
	"os"

	"github.com/MomirMilutinovic/NAiSP_Projekat/utils"
)

type IndexIterator struct {
	indexFile  *os.File
	end_offset int64
	Valid      bool
	Ok         bool
}

// Pravi IndexIterator koji iterira kroz indeks koji je realizovan kao zaseban fajl
// Ako je doslo do greske u otvaranju fajla vraca nil
func GetIndexIteratorFromIndexFile(filename string) *IndexIterator {
	indexFile, err := os.Open(filename)
	if err != nil {
		return nil
	}

	size := utils.GetFileSize(filename)

	if size == -1 {
		return nil
	}

	iter := IndexIterator{indexFile: indexFile, end_offset: size, Valid: true, Ok: true}

	return &iter
}

// Pravi IndexIterator koji iterira kroz indeks koji je u okviru SSTabele
// koja je zapisana kao jedan fajl
func GetIndexIteratorFromSSTableFile(filename string) *IndexIterator {
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

	startOfIndex := footer.IndexOffset
	endOfIndex := footer.SummaryOffset

	_, err = sstFile.Seek(startOfIndex, io.SeekStart)
	if err != nil {
		sstFile.Close()
		return nil
	}

	iter := &IndexIterator{indexFile: sstFile, end_offset: endOfIndex, Valid: true, Ok: true}
	return iter
}

// Funkcija cita sledeci zapis iz indeksa.
// Moze invalidirati iterator ako dodje do greske. U tom slucaju, atribut Ok
// se postavlja na false.
func (iter *IndexIterator) Next() *IndexEntry {
	cur_pos, _ := iter.indexFile.Seek(0, io.SeekCurrent)

	if !iter.Valid {
		return nil
	}

	if cur_pos >= iter.end_offset {
		iter.Valid = false
		iter.Ok = true
		iter.indexFile.Close()
		return nil
	}

	entry, ok := readIndexEntry(iter.indexFile)

	iter.Valid = (entry != nil)
	iter.Ok = ok

	if !iter.Valid {
		iter.indexFile.Close()
	}

	return entry
}

// Funkcija skace za offset bajtova u napred u fajlu koji se cita
// Ako dodje do greske invalidira iterator i postavlja Ok na false i zatvara fajl
func (iter *IndexIterator) SeekToOffset(offset int64) {
	_, err := iter.indexFile.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Println("Greska pri citanju indeksa: ", err)
		iter.Valid = false
		iter.Ok = false
		iter.indexFile.Close()
	}

}

// Postavlja end_offest
func (iter *IndexIterator) SetEndOffset(end_offset int64) {
	iter.end_offset = end_offset
}

// Trazi zapis u indeksu koji se poklapa sa datim kljucem i potom zatvara fajl i invalidira iterator
// Ako ga nadje u opsegu nad kojim je iterator zadat (pre end_offset) vraca IndexEntry sa tim zapisom
// Ako ga ne nadje vraca nil
func (iter *IndexIterator) SeekAndClose(key_string string) *IndexEntry {

	for currentIndexEntry := iter.Next(); iter.Valid; currentIndexEntry = iter.Next() {
		if currentIndexEntry.Key == key_string {
			iter.indexFile.Close()
			iter.Valid = false
			return currentIndexEntry
		}
	}

	iter.indexFile.Close()
	iter.Valid = false
	return nil
}

func (iter *IndexIterator) Close() {
	iter.indexFile.Close()
	iter.Valid = false
}
