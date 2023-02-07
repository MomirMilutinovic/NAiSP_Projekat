package sstable

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestSSTableSingleFile(t *testing.T) {
	fmt.Println("Kompajliralo se")

	sorted_entries := make([]*SSTableEntry, 0)
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("Darko"), []byte{'S', 'V', 50, 21}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("Gojko"), []byte{49, 21}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("Marko"), []byte("SV 38/2021")))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("Momir"), []byte{39, 21, 1, 2, 3}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("Vuk"), []byte{52, 21}))

	sstWriter := GetSSTFileWriter(false)
	sstWriter.Open("test_table_fused")

	for _, sstEntry := range sorted_entries {
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()
}

func TestReadWholeSSTableSingleFile(t *testing.T) {
	// Postoji nasumicna sansa da ovaj test ne prodje jer se magicni broj slucajno
	// pojavi u sred footer-a
	// Problem se resi kada obrisem tabelu i opet pokrenem testove.
	// Zasto se ovo dogadja? Nigde ne pisem po SSTabeli, a TestSSTableSingleFile treba da kreira novi fajl.
	// Razlog: O_CREATE flag ne brise fajlove i onda mogu ostati stari podaci
	iter := GetSSTableIterator("test_table_fused-Data.db")

	if iter == nil {
		t.Fatalf("Doslo je do greske u otvaranju sstabele")
	}

	number_of_entries := 0
	for entry := iter.Next(); iter.Valid; entry = iter.Next() {
		fmt.Println("Kljuc: ", string(entry.Key), " Vrednost: ", entry.Value)
		number_of_entries++
	}

	if number_of_entries != 5 {
		t.Fatalf("Nisu procitani svi redovi")
	}

	if !iter.Ok {
		t.Fatalf("Doslo je do greske u citanju sstable")
	}
}

func TestSSTableIteratorNonExistentFile(t *testing.T) {
	sstIter := GetSSTableIterator("nepostojeci_fajl")

	if sstIter != nil && sstIter.Valid {
		t.Fatalf("Ne bi trebalo da se dobije iterator od nepostojeceg fajla")
	}
}

func TestSSTableIteratorMagicNumberOnly(t *testing.T) {

	f, _ := ioutil.TempFile("", "malformed_sst")
	defer os.Remove(f.Name())
	binary.Write(f, binary.LittleEndian, SSTABALE_SINGLE_FILE_MAGIC_NUMBER)
	filename := f.Name()
	f.Close()

	sstIter := GetSSTableIterator(filename)

	if sstIter != nil && sstIter.Valid {
		t.Fatalf("Ne bi trebalo da se dobije iterator od loseg sst fajla")
	}

}

func TestSSTableIteratorMalfomedAndEmpty(t *testing.T) {

	f, _ := ioutil.TempFile("", "malformed_sst")
	defer os.Remove(f.Name())
	binary.Write(f, binary.LittleEndian, SSTABALE_SINGLE_FILE_MAGIC_NUMBER)
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	filename := f.Name()
	f.Close()

	sstIter := GetSSTableIterator(filename)

	if sstIter != nil && sstIter.Valid {
		t.Fatalf("Ne bi trebalo da se dobije iterator od loseg sst fajla")
	}
}

func TestReadSSTableByKeySingleFile(t *testing.T) {

	entry := ReadOneSSTEntryWithKey([]byte("Gojko"), "test_table_fused-Data.db", "", "", "")

	if entry == nil {
		t.Fatalf("Trebalo je da nadje entry")
	} else {
		if string(entry.Key) != "Gojko" {
			t.Fatalf("Kljuc se ne poklapa")
		}
	}

	entry = ReadOneSSTEntryWithKey([]byte("Vuk"), "test_table_fused-Data.db", "", "", "")

	if entry == nil {
		t.Fatalf("Trebalo je da nadje entry")
	} else {
		if string(entry.Key) != "Vuk" {
			t.Fatalf("Kljuc se ne poklapa")
		}
	}

	entry = ReadOneSSTEntryWithKey([]byte("Darko"), "test_table_fused-Data.db", "", "", "")

	if entry == nil {
		t.Fatalf("Trebalo je da nadje entry")
	} else {
		if string(entry.Key) != "Darko" {
			t.Fatalf("Kljuc se ne poklapa")
		}
	}

	entry = ReadOneSSTEntryWithKey([]byte("Momia"), "test_table_fused-Data.db", "", "", "")

	if entry != nil {
		t.Fatalf("Nije trebalo da nadje ovo")
	}

}

func TestWriteIntense(t *testing.T) {
	config.DefaultConfiguration.SummaryDensity = 128
	config.ReadConfig()

	entries := make([]*SSTableEntry, 0)

	for i := uint64(0); i < 20000; i += 2 {
		key := fmt.Sprintf("%05d", i)
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, i)
		entries = append(entries, CreateFreshSSTableEntry([]byte(key), value))
	}

	sstWriter := GetSSTFileWriter(false)
	sstWriter.Open("intense_table")

	for _, sstEntry := range entries {
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()
}

func TestReadIntense(t *testing.T) {

	// ~10000 neuspesnih citanja
	for i := uint64(1); i < 20000; i += 2 {
		key := []byte(fmt.Sprintf("%05d", i))
		entry := ReadOneSSTEntryWithKey(key, "intense_table-Data.db", "", "", "")
		if entry != nil {
			t.Fatalf("Nije trebalo da nadje kljuc %s", key)
		}
	}

	// 10000 uspesnih citanja - Glavni uticaj na sporocu ovog testa ako je summary_density mali (3)
	// Ovi prvi ni ne prodju filter
	for i := uint64(0); i < 20000; i += 2 {
		key := []byte(fmt.Sprintf("%05d", i))
		entry := ReadOneSSTEntryWithKey(key, "intense_table-Data.db", "", "", "")
		if entry == nil {
			t.Fatalf("Trebalo da nadje kljuc %s", key)
		} else if entry.Value[0] != byte(i%256) {
			t.Fatalf("Nije dobro porictana vrednost")
		}
	}
}

func Test100000RandomStrings(t *testing.T) {
	rand.Seed(time.Now().Unix())
	length := 10

	for rep := 0; rep < 100000; rep++ {
		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		if ReadOneSSTEntryWithKey(ran_str, "intense_table-Data.db", "", "", "") != nil {
			t.Fatalf("Nije trebalo da nadje kljuc %s", ran_str)
		}
	}
}
