package sstable

import (
	"io"
	"os"

	"github.com/MomirMilutinovic/NAiSP_Projekat/utils"
)

type SSTableIterator struct {
	sstFile    *os.File
	Valid      bool  // Validnost iteratora, ako se doslo do kraja tablele i greske bice false
	Ok         bool  // True ako je nema greske, false ako je doslo do greske
	end_offset int64 // Pozicija u sstFile na kojem se zavrsava tabela
}

// Kako napraviti ovaj iterator
// Treba da znamo koju SST citamo (ime fajla)

// Treba nam da li je jedan ili vise fajlova
// Ovo znamo na osnovu formata
// Drugaciji magicni brojevi

// Kako dobijamo end_offset
// Na osnovu magicnog broja
// Ako je zaseban fajl - velicina - 8
// Ako nije zaseban fajl - imamo metaindex

// Cita sledeci zapis iz SSTabele
// Vraca procitani zapis ili nil ako nema vise zapisa ili ako je doslo do greske
// Atribut Valid se postavlja false ako nema vise zapisa ili ako je doslo do greske
// Ako je doslo do greske atribut Ok se postavlja na false
func (iter *SSTableIterator) Next() *SSTableEntry {
	cur_pos, _ := iter.sstFile.Seek(0, io.SeekCurrent)

	if !iter.Valid {
		return nil
	}

	if cur_pos >= iter.end_offset {
		iter.Valid = false
		iter.Ok = true
		iter.sstFile.Close()
		return nil
	}

	entry, ok := ReadOneSSTEntry(iter.sstFile)

	iter.Valid = (entry != nil)
	iter.Ok = ok

	if !iter.Valid {
		iter.sstFile.Close()
	}

	return entry

}

// Funkcija postavlja iterator na odredjenu poziciju u fajlu
// Ne vrsi provere granica sstabele
// Ako dodje do greske postavlja Valid i Ok na false
func (iter *SSTableIterator) SeekToOffset(offset int64) {
	_, err := iter.sstFile.Seek(offset, io.SeekStart)
	if err != nil {
		iter.Valid = false
		iter.Ok = false
		iter.sstFile.Close()
	}

}

// Funkcija cita zapise iz sstabele dok ne naidje na kraj opsega iz kog moze
// citati ili dok ne nadje zapis sa datim kljucem. Potom zatvara fajl i invalidira iterator.
// Postavlja Ok na false ako dodje do greske u citanju fajla.
// Vraca zapis iz SSTabele ako je nadjen ili nil
func (iter *SSTableIterator) SeekAndClose(key []byte) *SSTableEntry {

	//TODO: Mozda ne bi trebalo da radimo ova silna pretvaranja u stringove
	key_string := string(key)
	//defer iter.sstFile.Close()

	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		if string(entry.Key) == key_string {
			return entry
		}
	}

	return nil
}

// Zatvara fajl iteratora i invalidira ga
func (iter *SSTableIterator) Close() {
	iter.sstFile.Close()
	iter.Valid = false
}

func (iter *SSTableIterator) Tell() int64 {
	offset, err := iter.sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		iter.Close()
		return -1
	}

	return offset

}

// Funkcija konstruise iterator SSTabele koja se nalazi u fajlu filename.
// Funkcija sama detektuje vrstu SSTabele.
// Vraca nil ako dodje do greske ili ako je prosledjen naziv fajla koji ne
// predstavlja SSTabelu.
func GetSSTableIterator(filename string) *SSTableIterator {
	//TODO: Osigurati da se zatvara fajl nakon return nil
	sstFile, err := os.Open(filename)
	if err != nil {
		return nil
	}

	magic_number := readMagicNumber(sstFile)

	size := utils.GetFileSize(filename)

	if size == -1 {
		return nil
	}

	if magic_number == SSTABLE_MULTI_FILE_MAGIC_NUMBER {
		end_of_sstable := size - SSTABLE_MAGIC_NUMBER_SIZE
		_, err := sstFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil
		}

		iter := SSTableIterator{sstFile: sstFile, end_offset: end_of_sstable, Valid: true, Ok: true}
		return &iter

	} else if magic_number == SSTABALE_SINGLE_FILE_MAGIC_NUMBER {
		footer := ReadSSTFooter(sstFile)
		if footer == nil {
			return nil
		}

		// Moramo se vratiti na pocetak nakon citanja footer-a
		_, err = sstFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil
		}

		iter := SSTableIterator{sstFile: sstFile, end_offset: footer.IndexOffset, Valid: true, Ok: true}
		return &iter

	} else {
		return nil // Sta god da smo procitali nije sstabela
	}

	return nil

}
