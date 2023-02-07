package engine

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestHyperLogLogType(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()
	hll_key := "mojHLL"
	db.CreateHyperLogLog(hll_key, 15)
	db.Put("2", []byte{})

	number_of_elements := 1000
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		db.AddValueToHyperLogLog(hll_key, string(ran_str))

		if i%1000 == 0 {
			fmt.Println(i, "/", 10000)
		}
	}

	estimate, _ := db.EstimateHyperLogLog(hll_key)

	if math.Abs(estimate-float64(number_of_elements)) > float64(number_of_elements)/10 {
		t.Fatalf("Procena broja elemenata nije dobra")
	}

	Cleanup()
}

func TestBatchAddHLL(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()
	hll_key := "mojHLL"
	db.CreateHyperLogLog(hll_key, 12)
	db.Put("2", []byte{})

	number_of_elements := 10000
	elmements := make([]string, 0)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elmements = append(elmements, string(ran_str))

		if i%1000 == 0 {
			fmt.Println(i, "/", 10000)
		}
	}

	db.AddBatchOfValuesToHyperLogLog(hll_key, elmements)

	estimate, _ := db.EstimateHyperLogLog(hll_key)

	if math.Abs(estimate-float64(number_of_elements)) > float64(number_of_elements)/10 {
		t.Fatalf("Procena broja elemenata nije dobra")
	}

	Cleanup()
}

func TestInvalidParams(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()
	hll_key := "mojHLL"

	if db.CreateHyperLogLog(hll_key, 2) {
		t.Fatalf("Ne bi trebalo da moze da se napravi HLL sa preciznoscu manjom od minimalne.")
	}

	if db.CreateHyperLogLog(hll_key, 100) {
		t.Fatalf("Ne bi trebalo da moze da se napravi HLL sa preciznoscu vecom od maksimalne")
	}

	Cleanup()
}
