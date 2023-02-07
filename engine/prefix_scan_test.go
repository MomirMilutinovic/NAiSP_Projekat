package engine

import (
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestPrefixScan(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	max_temps_ns := db.List("MaxTemp/Novi Sad", 1, 6)
	if len(max_temps_ns) != 4 || max_temps_ns[0][0] != 1 || max_temps_ns[1][0] != 1 || max_temps_ns[2][0] != 1 || max_temps_ns[3][0] != 31 {
		t.Fatalf("Nesto se nije dobro procitalo")
	}

	// Trazimo vlaznost vazduha za februar
	hum_bg := db.List("Humidity/Belgrade/2023-02", 1, 6)
	if len(hum_bg) != 3 || hum_bg[0][0] != 4 || hum_bg[1][0] != 3 || hum_bg[2][0] != 5 {
		t.Fatalf("Nesto se nije dobro procitalo")
	}

	Cleanup()
}

func TestInvalidParameters(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	if len(db.List("MaxTemp/", 0, 45)) != 0 {
		t.Fatalf("Nije tacan broj rezultata")
	}

	if len(db.List("MaxTemp/", 1, 0)) != 0 {
		t.Fatalf("Nije tacan broj rezultata")
	}

	if len(db.List("MaxTemp/", 156, 1)) != 0 {
		t.Fatalf("Nije tacan broj rezultata")
	}

	Cleanup()
}

func TestNoResults(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	if len(db.List("MaxTmp", 1, 45)) != 0 {
		t.Fatalf("Nije tacan broj rezultata")
	}

	if len(db.List("MbxTemp", 1, 45)) != 0 {
		t.Fatalf("Nije tacan broj rezultata")
	}

	Cleanup()
}

func TestWholeStringPrefix(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 10
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	result := db.List("Humidity/Novi Sad/2023-07-07", 1, 54)
	if len(result) != 1 || result[0][0] != 1 {
		t.Fatalf("Nije dobar rezulatat")
	}

	result = db.List("Humidity/Novi Sad/2023-07-07 ", 1, 54)
	if len(result) != 0 {
		t.Fatalf("Nije dobar rezulatat")
	}

	result = db.List("Humidity/Novi Sad/2023-06-07", 1, 54)
	if len(result) != 0 {
		t.Fatalf("Nije dobar rezulatat")
	}

	Cleanup()
}

func TestInteractionWithDeletionsAndEdits(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 4
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()

	db := GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	db.Get("Humidity/Novi Sad/2023-02-06")

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{15})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{13})
	db.Delete("Humidity/Novi Sad/2023-02-06")
	db.Delete("Humidity/Belgrade/2023-02-06")

	db.Put("Humidity/Novi Sad/2023-02-06", []byte{51})
	db.Delete("Humidity/Belgrade/2023-02-06")
	db.Delete("Humidity/Belgrade/2023-02-05")
	db.Put("Humidity/Belgrade/2023-02-06", []byte{23})

	result := db.List("Humidity/Novi Sad", 1, 5)
	if len(result) != 4 || result[0][0] != 3 || result[1][0] != 51 || result[2][0] != 5 || result[3][0] != 1 {
		t.Fatalf("Nije dobar rezulatat")
	}

	result = db.List("Humidity/Belgrade/", 1, 54)
	if len(result) != 3 || result[0][0] != 23 {
		t.Fatalf("Nije dobar rezulatat")
	}

	Cleanup()

}
