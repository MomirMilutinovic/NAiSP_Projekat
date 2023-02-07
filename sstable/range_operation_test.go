package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestRangeScan(t *testing.T) {
	Cleanup()
	config.DefaultConfiguration.SummaryDensity = 3 // Smanjujemo summary density da bi se adekvatno testirale provere summary-a
	config.ReadConfig()

	sorted_entries := make([]*SSTableEntry, 0)
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-06"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-07"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-08"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-01-01"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-01-02"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-05-05"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-01-17"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-04-03"), []byte{}))

	sstWriter := GetSSTFileWriter(false)
	sstWriter.Open("test_table_date_search")

	for _, sstEntry := range sorted_entries {
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()

	if len(RangeScanSSTable("2020-01-01", "2024-01-01", "test_table_date_search-Data.db", "", "", "")) != 8 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("1", "2", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2", "3", "test_table_date_search-Data.db", "", "", "")) != 8 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2", "3", "test_table_date_search-Data.db", "", "", "")) != 8 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2022-01-01", "2022-02-01", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2022-01-01", "2022-01-02", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2022-01-01", "2022-01-01", "test_table_date_search-Data.db", "", "", "")) != 1 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2020-01-01", "2020-12-31", "test_table_date_search-Data.db", "", "", "")) != 3 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}

	if len(RangeScanSSTable("2023-01-01", "2023-12-31", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije ucitan ispravan broj zapisa")
	}
}

func TestPrefixScan(t *testing.T) {
	Cleanup()
	config.DefaultConfiguration.SummaryDensity = 3 // Smanjujemo summary density da bi se adekvatno testirale provere summary-a
	config.ReadConfig()

	fmt.Println(config.Configuration.SummaryDensity)

	sorted_entries := make([]*SSTableEntry, 0)
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-06"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-07"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2020-04-08"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-01-01"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-01-02"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-05-05"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2022-05-15"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-01-07"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-01-17"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-01-19"), []byte{}))
	sorted_entries = append(sorted_entries, CreateFreshSSTableEntry([]byte("2023-03-03"), []byte{}))

	sstWriter := GetSSTFileWriter(false)
	sstWriter.Open("test_table_date_search")

	for _, sstEntry := range sorted_entries {
		sstWriter.Put(sstEntry)
	}

	sstWriter.Finish()

	if len(PrefixScanSSTable("", "test_table_date_search-Data.db", "", "", "")) != 11 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2", "test_table_date_search-Data.db", "", "", "")) != 11 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("1", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2020", "test_table_date_search-Data.db", "", "", "")) != 3 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2021", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2022", "test_table_date_search-Data.db", "", "", "")) != 4 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2023", "test_table_date_search-Data.db", "", "", "")) != 4 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2022-05", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2023-01", "test_table_date_search-Data.db", "", "", "")) != 3 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2022-02", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2023-01-1", "test_table_date_search-Data.db", "", "", "")) != 2 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2023-01-0", "test_table_date_search-Data.db", "", "", "")) != 1 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2022-01-1", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("sdfasd", "test_table_date_search-Data.db", "", "", "")) != 0 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}

	if len(PrefixScanSSTable("2023-01-17", "test_table_date_search-Data.db", "", "", "")) != 1 {
		t.Fatalf("Nije procitan ispravan broj vrednosti")
	}
}

func Cleanup() {

	// Brisanje fajlova od proslih testova
	os.RemoveAll("wal")
	DeleteSSTables()
}

func DeleteSSTables() {
	// Brisemo sve SSTabele
	files, err := filepath.Glob("*.db")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	files, err = filepath.Glob("*Metadata.txt")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}
