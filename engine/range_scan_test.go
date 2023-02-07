package engine

import (
	"fmt"
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestRangeScan(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	if len(db.RangeScan("1", "0", 1, 100)) != 0 {
		t.Fatalf("Nevalidan opseg nije trebao da vrati nista")
	}

	if len(db.RangeScan("000", "009", 1, 100)) != 10 {
		t.Fatalf("Nije vracen ispravan broj vrednosti")
	}

	if len(db.RangeScan("190", "200", 1, 100)) != 11 {
		t.Fatalf("Nije vracen ispravan broj vrednosti")
	}

	if len(db.RangeScan("0", "1", 1, 100)) != 100 {
		t.Fatalf("Nije vracen ispravan broj vrednosti")
	}

	Cleanup()

}

func TestRangeScanPagination(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	if len(db.RangeScan("001", "010", 1, 6)) != 6 {
		t.Fatalf("Paginacija nije izvrsena kako treba")
	}

	if len(db.RangeScan("001", "010", 2, 6)) != 4 {
		t.Fatalf("Paginacija nije izvrsena kako treba za poslednju stranicu")
	}

	// Paginacija sa stranicama velicine 3
	if len(db.RangeScan("001", "010", 1, 3)) != 3 {
		t.Fatalf("Paginacija nije izvrsena kako treba")
	}

	if len(db.RangeScan("001", "010", 2, 3)) != 3 {
		t.Fatalf("Paginacija nije izvrsena kako treba")
	}

	if len(db.RangeScan("001", "010", 3, 3)) != 3 {
		t.Fatalf("Paginacija nije izvrsena kako treba")
	}

	if len(db.RangeScan("001", "010", 4, 3)) != 1 {
		t.Fatalf("Ova strana bi trebalo da nije popunjena")
	}

	// Stranica koja ne postoji
	if len(db.RangeScan("001", "010", 5, 3)) != 0 {
		t.Fatalf("Ova strana ne bi trebalo da postoji")
	}

	if len(db.RangeScan("001", "010", 6, 2)) != 0 {
		t.Fatalf("Ova strana ne bi trebalo da postoji")
	}

	if len(db.RangeScan("191", "200", 11, 1)) != 0 {
		t.Fatalf("Ova strana ne bi trebalo da postoji")
	}

	Cleanup()

}

func TestRangeScanPaginationIterative(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	total_number_of_results := 0
	page_size := 11 // Uzimamo broj koji nije deljiv sa brojem rezultata
	all_results := make([][]byte, 0)
	page_number := 1

	for results := db.RangeScan("001", "200", uint(page_number), uint(page_size)); len(results) > 0; {
		total_number_of_results += len(results)

		all_results = append(all_results, results...)
		page_number++
		results = db.RangeScan("001", "200", uint(page_number), uint(page_size))
	}

	fmt.Println("Brojevi 1-200", all_results)

	if total_number_of_results != 200 {
		t.Fatalf("Nije vracen ispravan broj rezultata")
	}

	Cleanup()
}

func TestRangeScanInvalidParameters(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	if len(db.RangeScan("103", "003", 1, 1)) != 0 {
		t.Fatalf("Nevalidan opseg ne bi trebalo da vrati nikakav rezulatat")
	}

	if len(db.RangeScan("001", "003", 0, 100)) != 0 {
		t.Fatalf("Nulta stranica ne bi trebalo da postoji")
	}

	if len(db.RangeScan("001", "003", 1, 0)) != 0 {
		t.Fatalf("Velicina stranice ne moze biti nula")
	}

	Cleanup()

}

func TestRangeScanEqualBounds(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	if len(db.RangeScan("001", "001", 1, 2000)) != 1 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	if len(db.RangeScan("200", "200", 1, 2000)) != 1 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	if len(db.RangeScan("127", "127", 1, 2000)) != 1 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	if len(db.RangeScan("34234", "34234", 1, 2000)) != 0 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	Cleanup()

}

func TestNotSubsetButHasIntersection(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	if len(db.RangeScan("", "001", 1, 2000)) != 2 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	if len(db.RangeScan("191", "300", 1, 2000)) != 10 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	Cleanup()
}

func TestRangeScanNoResults(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	// Stavljamo kljuceve od 10 da bi smo imali intervale sa obe strane koji ne
	// obuhvataju unete podatke
	for i := 10; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	if len(db.RangeScan("", "009", 1, 2000)) != 0 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	if len(db.RangeScan("21", "300", 1, 2000)) != 0 {
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	Cleanup()
}

func TestRangeScanInteractionWithDeletions(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	// Stavljamo kljuceve od 10 da bi smo imali intervale sa obe strane koji ne
	// obuhvataju unete podatke
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	// Treba jos 9 kljuceva da izazovemo flush
	db.Delete("001")
	db.Delete("002")
	db.Delete("003")
	db.Delete("004")
	db.Delete("005")
	db.Delete("006")
	db.Delete("007")
	db.Delete("008")
	db.Delete("009")

	// Ovi delete-ovi ce biti u MemTabeli
	// U sstabelama ovi kljucevi nisu obrisani
	db.Delete("101")
	db.Delete("010")
	db.Delete("145")

	if len(db.RangeScan("001", "200", 1, 2000)) != 200-12 {
		fmt.Println(len(db.RangeScan("001", "200", 1, 2000)))
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	Cleanup()
}

func TestRangeScanInteractionWithDeletionsAndEditsButSSTableIsOneFile(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = false
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	// Stavljamo kljuceve od 10 da bi smo imali intervale sa obe strane koji ne
	// obuhvataju unete podatke
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	// Treba jos 9 kljuceva da izazovemo flush
	db.Put("001", []byte{1})
	db.Delete("002")
	db.Delete("003")
	db.Delete("004")
	db.Delete("005")
	db.Delete("006")
	db.Delete("007")
	db.Delete("008")
	db.Delete("009")

	// Ovi delete-ovi ce biti u MemTabeli
	// U sstabelama ovi kljucevi nisu obrisani
	db.Delete("101")
	db.Delete("010")
	db.Delete("145")
	db.Put("002", []byte{2})

	results := db.RangeScan("001", "200", 1, 2000)
	if len(results) != 200-10 { // 11 brisanja, ali 002 smo vratili
		fmt.Println(results)
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	// Kljuc: 1 Vrednost: 1
	if results[0][0] != 1 {
		t.Fatalf("Nije stavljena aktuelna vrednost u rezultat")
	}

	//Kljuc: 2 Vrednost: 2
	if results[1][0] != 2 {
		t.Fatalf("Nije stavljena aktuelna vrednost u rezultat")
	}

	Cleanup()
}

func TestRangeScanInteractionWithDeletionsAndEdits(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()

	Cleanup()

	db := GetNewDB()
	// Stavljamo kljuceve od 10 da bi smo imali intervale sa obe strane koji ne
	// obuhvataju unete podatke
	for i := 0; i <= 200; i++ {
		db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
	}

	// Treba jos 9 kljuceva da izazovemo flush
	db.Put("001", []byte{11})
	db.Delete("002")
	db.Delete("003")
	db.Delete("004")
	db.Delete("005")
	db.Delete("006")
	db.Delete("007")
	db.Delete("008")
	db.Delete("009")

	// Ovi delete-ovi ce biti u MemTabeli
	// U sstabelama ovi kljucevi nisu obrisani
	db.Delete("101")
	db.Delete("010")
	db.Delete("145")
	db.Put("002", []byte{22})

	results := db.RangeScan("001", "200", 1, 2000)
	if len(results) != 200-10 { // 11 brisanja, ali 002 smo vratili
		fmt.Println(results)
		t.Fatalf("Nije uctian ispravan broj zapisa")
	}

	// Kljuc: 1 Vrednost: 11
	if results[0][0] != 11 {
		t.Fatalf("Nije stavljena aktuelna vrednost u rezultat")
	}

	//Kljuc: 2 Vrednost: 22
	if results[1][0] != 22 {
		t.Fatalf("Nije stavljena aktuelna vrednost u rezultat")
	}

	Cleanup()
}
