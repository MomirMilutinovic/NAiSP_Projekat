package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestEngineSimple(t *testing.T) {
	// Brisanje fajlova od proslih testova
	Cleanup()

	db := GetNewDB()

	for i := 0; i < 100; i++ {
		ok := db.Put(fmt.Sprintf("%03d", i), []byte{uint8(i)})
		if !ok {
			t.Fatalf("Nesto je poslo po zlu")
		}
	}

	db.Put("101", []byte{101})
	if db.Get("101") == nil {
		t.Fatalf("Trebalo je da nadje ovo")
	}
	db.Put("101", []byte{250})
	db.Put("102", []byte{102})
	db.Put("103", []byte{103})
	db.Put("104", []byte{104}) // Ovde ce biti flush

	if db.Get("101")[0] != 250 {
		t.Fatalf("Pogresno je procitana vrednost")
	}

	for i := 0; i < 100; i++ {
		val := db.Get(fmt.Sprintf("%03d", i))
		if val == nil {
			t.Fatalf("Trebalo je da nadje ovo")
		}
	}

}

func TestEngineWithDeletions(t *testing.T) {
	// Brisanje fajlova od proslih testova
	Cleanup()

	db := GetNewDB()
	db.Put("102", []byte{102})
	if db.Delete("102") == false {
		t.Fatalf("Trebalo da je obrise")
	}

	if db.Get("102") != nil {
		fmt.Println(db.Get("101"))
		t.Fatalf("Nije trebalo da nadje ovo")
	}

	db.Put("101", []byte{101})
	db.Put("103", []byte{103})
	db.Put("104", []byte{104})

	if db.Get("102") != nil {
		fmt.Println(db.Get("102"))
		t.Fatalf("Nije trebalo da nadje ovo")
	}

	if db.Get("101") == nil {
		t.Fatalf("Trebalo je da nadje ovo")
	}

}

func TestForStaleCache(t *testing.T) {
	// Brisanje fajlova od proslih testova
	Cleanup()

	db := GetNewDB()
	db.Put("101", []byte{101})
	db.Put("102", []byte{102})
	db.Put("103", []byte{101})
	db.Put("104", []byte{102})
	// Flush

	// Sad je 101 u kesu
	if db.Get("101") == nil {
		t.Fatalf("Trebalo je ovo da nadje")
	}

	db.Put("101", []byte{201})
	db.Put("102", []byte{102})
	db.Put("103", []byte{101})
	db.Put("104", []byte{102})
	//Flush

	val := db.Get("101")
	if val == nil {
		t.Fatalf("Trebalo je ovo da nadje")
	} else if val[0] != 201 {
		t.Fatalf("Nije pronadjena dobra vrednost")
	}

	if db.Get("102") == nil {
		t.Fatalf("Trebalo je ovo da nadje")
	} // 102 je sada u kesu

	db.Put("101", []byte{102})
	db.Delete("102")
	db.Put("103", []byte{101})
	db.Put("104", []byte{102})
	//Flush

	val = db.Get("102") // 102 je obrisan
	if val != nil {
		t.Fatalf("Nije trebalo je ovo da nadje")
	}

}

func TestWALReplayMemTableOnly(t *testing.T) {
	Cleanup()

	db := GetNewDB()

	// 3 zapisa se nece flush-ovati
	if !db.Put("101", []byte{101}) {
		t.Fatalf("Ovo je trebalo da prodje")
	}
	if !db.Put("102", []byte{102}) {
		t.Fatalf("Ovo je trebalo da prodje")
	}
	if !db.Delete("101") {
		t.Fatalf("Ovo je treblo da prodje")
	}

	if !db.Put("103", []byte{103}) {
		t.Fatalf("Ovo je trebalo da prodje")
	}

	// Simulacija restarta sistema
	db = GetNewDB()

	if db.Get("101") != nil {
		t.Fatalf("Ovaj kluc je bio obrisan")
	}

	val := db.Get("102")
	if val == nil {
		t.Fatalf("Ovaj kluc bi trebalo da je prisutan")
	} else if val[0] != 102 {
		t.Fatalf("Nije dobro procitana vrednost")
	}

	val = db.Get("103")
	if val == nil {
		t.Fatalf("Ovaj kluc bi trebalo da je prisutan")
	} else if val[0] != 103 {
		t.Fatalf("Nije dobro procitana vrednost")
	}

}

func TestWriteALotOfData(t *testing.T) {
	Cleanup()

	config.DefaultConfiguration.WalSize = 10000   // Smanjemo velicinu wal segmenta da bi ih bilo vise
	config.DefaultConfiguration.MemtableSize = 40 // Da ubrza stvari
	db := GetNewDB()

	// Dodajemo 1000 brojeva, ali uvek brisemo prethodni parni ako mozemo
	for i := uint64(0); i < 1000; i++ {
		bytes_to_write := bytes.NewBuffer(make([]byte, 0))
		err := binary.Write(bytes_to_write, binary.LittleEndian, i)
		if err != nil {
			t.Fatalf("Ovo nije trebalo da se desi")
		}
		if !db.Put(fmt.Sprintf("%04d", i), bytes_to_write.Bytes()) {
			t.Fatalf("Ovo je trebalo da prodje")
		}

		if i >= 2 && i%2 == 0 {
			if !db.Delete(fmt.Sprintf("%04d", i-2)) {
				t.Fatalf("Ovo je trebalo da prodje")
			}
		}
	}
	for i := uint64(0); i < 998; i++ {
		val := db.Get(fmt.Sprintf("%04d", i))

		if val != nil && i%2 == 0 {
			t.Fatalf("Parni brojevi su bili obrisnani %d", i)
		} else if val == nil && i%2 == 1 {
			t.Fatalf("Neparni brojevi bi treblo da su tu %d", i)
		}
	}

}

func TestWALReplayWithALotOfData(t *testing.T) {
	Cleanup()

	config.DefaultConfiguration.WalSize = 10000   // Smanjemo velicinu wal segmenta da bi ih bilo vise
	config.DefaultConfiguration.MemtableSize = 40 // Da ubrza stvari
	db := GetNewDB()

	// Dodajemo 200 brojeva, ali uvek brisemo prethodni parni ako mozemo
	for i := uint64(0); i < 200; i++ {
		bytes_to_write := bytes.NewBuffer(make([]byte, 0))
		err := binary.Write(bytes_to_write, binary.LittleEndian, i)
		if err != nil {
			t.Fatalf("Ovo nije trebalo da se desi")
		}
		if !db.Put(fmt.Sprintf("%04d", i), bytes_to_write.Bytes()) {
			t.Fatalf("Ovo je trebalo da prodje")
		}

		if i >= 2 && i%2 == 0 {
			if !db.Delete(fmt.Sprintf("%04d", i-2)) {
				t.Fatalf("Ovo je trebalo da prodje")
			}
		}
	}

	// Brisemo SSTabele da ih read path ne bi pokupio
	DeleteSSTables()
	//Simulacija restarta sistema
	db = GetNewDB()

	// Moramo izostaviti 9998 jer on nece biti obrisan
	for i := uint64(193); i < 198; i++ {
		val := db.Get(fmt.Sprintf("%04d", i))

		if val != nil && i%2 == 0 {
			t.Fatalf("Parni brojevi su bili obrisnani %d", i)
		} else if val == nil && i%2 == 1 {
			t.Fatalf("Neparni brojevi bi treblo da su tu %d", i)
		}
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

	files, err = filepath.Glob("*.txt")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}
