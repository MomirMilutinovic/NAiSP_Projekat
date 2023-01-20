package bloomfilter

import (
	"testing"
)

func TestWords(t *testing.T) {
	// Test primer preuzet sa https://www.geeksforgeeks.org/bloom-filters-introduction-and-python-implementation/

	// Reci koje se dodaju
	word_present := []string{"abound", "abounds", "abundance", "abundant", "accessible",
		"bloom", "blossom", "bolster", "bonny", "bonus", "bonuses",
		"coherent", "cohesive", "colorful", "comely", "comfort",
		"gems", "generosity", "generous", "generously", "genial"}

	// Reci koje se ne dodaju
	word_absent := []string{"bluff", "cheater", "hate", "war", "humanity",
		"racism", "hurt", "nuke", "gloomy", "facebook",
		"geeksforgeeks", "twitter"}

	falsePositiveRate := 0.05

	bloomFilter := CreateBloomFilterBasedOnParams(len(word_present), falsePositiveRate)
	t.Log("Duzina niza bitova: ", bloomFilter.BitArrayLen)
	t.Log("Broj hes funkcija: ", bloomFilter.HashFunctionCount)
	t.Log("False-positive rate: ", falsePositiveRate)

	for _, word := range word_present {
		bloomFilter.add([]byte(word))
	}

	for _, word := range word_present {
		found := bloomFilter.find([]byte(word))
		if !found {
			t.Fatalf(word, " je trebao da bude nadjen, a nije")
		} else {
			t.Log(word, " nadjen")
		}
	}

	for _, word := range word_absent {
		found := bloomFilter.find([]byte(word))
		if !found {
			t.Log(word, " nije nadjen")
		} else {
			t.Log(word, " je false-positive")
		}
	}

}
