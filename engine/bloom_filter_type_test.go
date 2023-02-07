package engine

import (
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestBloomFilterType(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	bf_key := "mojFilter"
	db.CreateBloomFilter(bf_key, 76, 4)
	db.Put("1", []byte{}) // Flush bloom filter

	// Reci koje se dodaju
	word_present := []string{"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial"}

	// Reci koje se ne dodaju
	word_absent := []string{"bluff", "cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter"}

	for _, word := range word_present {
		db.AddStringToBloomFilter(bf_key, word)
	}

	for _, word := range word_present {
		found, _ := db.IsStringPossiblyInBloomFilter(bf_key, word)
		if !found {
			t.Fatalf("%s je trebao da bude nadjen, a nije", word)
		} else {
			t.Log(word, " nadjen")
		}
	}

	for _, word := range word_absent {
		found, _ := db.IsStringPossiblyInBloomFilter(bf_key, word)
		if !found {
			t.Log(word, " nije nadjen")
		} else {
			t.Log(word, " je false-positive")
		}
	}

	Cleanup()

}

func TestBloomFilterTypeBatch(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	bf_key := "mojFilter"
	db.CreateBloomFilter(bf_key, 76, 4)
	db.Put("1", []byte{}) // Flush bloom filter

	// Reci koje se dodaju
	word_present := []string{"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial"}

	// Reci koje se ne dodaju
	word_absent := []string{"bluff", "cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter"}

	db.AddBatchOfStringToBloomFilter(bf_key, word_present)

	for _, word := range word_present {
		found, _ := db.IsStringPossiblyInBloomFilter(bf_key, word)
		if !found {
			t.Fatalf("%s je trebao da bude nadjen, a nije", word)
		} else {
			t.Log(word, " nadjen")
		}
	}

	for _, word := range word_absent {
		found, _ := db.IsStringPossiblyInBloomFilter(bf_key, word)
		if !found {
			t.Log(word, " nije nadjen")
		} else {
			t.Log(word, " je false-positive")
		}
	}

	Cleanup()

}

func TestBloomFilterInvalidParams(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	bf_key := "mojFilter"

	if db.CreateBloomFilter(bf_key, 0, 0) {
		t.Fatalf("Nisu validni parametri")
	}

	if db.CreateBloomFilter(bf_key, 16, 0) {
		t.Fatalf("Nisu validni parametri")
	}

	if db.CreateBloomFilter(bf_key, 0, 4) {
		t.Fatalf("Nisu validni parametri")
	}

	if !db.CreateBloomFilter(bf_key, 1, 1) {
		t.Fatalf("Jesu validni parametri, a nije proslo")
	}

	Cleanup()

}
