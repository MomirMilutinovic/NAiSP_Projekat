package engine

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	lsmtree "github.com/MomirMilutinovic/NAiSP_Projekat/LSMTree"
	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
	"github.com/MomirMilutinovic/NAiSP_Projekat/sstable"
)

func TestSTCS(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 100
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "size_tiered"
	config.DefaultConfiguration.SummaryDensity = 50
	config.ReadConfig()
	Cleanup()
	defer Cleanup()

	db := GetNewDB()

	number_of_elements := 6000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
		}
	}

	db.RunCompaction()

	for _, key := range elements {
		if db.Get(key) == nil {
			t.Fatalf("Ovo je trebalo da nadje")
		}
	}

}

func TestLCS(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 100
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "leveled"
	config.DefaultConfiguration.SummaryDensity = 50
	config.DefaultConfiguration.MultipleFileSSTable = false
	config.ReadConfig()
	Cleanup()
	defer Cleanup()

	db := GetNewDB()

	number_of_elements := 6000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
		}
	}

	db.RunCompaction()

	for _, key := range elements {
		if db.Get(key) == nil {
			t.Fatalf("Ovo je trebalo da nadje")
		}
	}

}

func TestSTCSMidAdd(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 100
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "size_tiered"
	config.DefaultConfiguration.SummaryDensity = 50
	config.DefaultConfiguration.MultipleFileSSTable = false
	config.ReadConfig()
	Cleanup()
	//defer Cleanup()

	db := GetNewDB()

	number_of_elements := 6000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
			db.RunCompaction()
		}
	}

	db.RunCompaction()

	for _, key := range elements {
		if db.Get(key) == nil {
			t.Fatalf("Ovo je trebalo da nadje")
		}
	}

}

func TestLCSMidAdd(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 100
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "leveled"
	config.DefaultConfiguration.SummaryDensity = 50
	config.DefaultConfiguration.MultipleFileSSTable = false
	config.ReadConfig()
	Cleanup()
	//defer Cleanup()

	db := GetNewDB()

	number_of_elements := 6000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%100 == 0 {
			db.RunCompaction()
		}

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
		}
	}

	//db.RunCompaction()

	for _, key := range elements {
		if db.Get(key) == nil {
			t.Fatalf("Ovo je trebalo da nadje")
		}
	}
	for level := 1; level <= lsmtree.Findlevel() && level < int(config.Configuration.LSMTreeLevels); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		fmt.Println("Nivo", level)
		tables, err := filepath.Glob("level-" + fmt.Sprintf("%02d", level) + "-usertable-*-Data.db") //Izdvajanje svih tabela trenutnog nivoa, za kompaktovanje
		if err != nil {
			panic(err)
		}

		for _, table := range tables {
			summaryIter := sstable.GetSummaryIteratorFromSSTableFile(table)
			fmt.Println(summaryIter.Begin_key, " ", summaryIter.End_key)
			summaryIter.Close()
		}
	}
}

func TestLCSMidAddSummaryPrintOnly(t *testing.T) {
	// Dodaje 6000 random kljuceva duzine 100 u bazu i proverava da li svi mogu da se uzmu nakon
	// kompakcije
	config.DefaultConfiguration.MemtableSize = 100
	config.DefaultConfiguration.MemtableStructure = "b_tree"
	config.DefaultConfiguration.CompactionStrategy = "leveled"
	config.DefaultConfiguration.SummaryDensity = 50
	config.DefaultConfiguration.MultipleFileSSTable = false
	config.ReadConfig()
	Cleanup()
	//defer Cleanup()

	db := GetNewDB()

	number_of_elements := 60000
	elements := make([]string, 0, number_of_elements)
	for i := 0; i < number_of_elements; i++ {

		length := 100

		ran_str := make([]byte, length)

		// Generating Random string
		for i := 0; i < length; i++ {
			ran_str[i] = byte(65 + rand.Intn(25))
		}

		elements = append(elements, string(ran_str))

		value := make([]byte, 0, 4)
		binary.LittleEndian.AppendUint32(value, uint32(i))

		db.Put(string(ran_str), value)

		if i%100 == 0 {
			db.RunCompaction()
		}

		if i%1000 == 0 {
			fmt.Println(i, "/", number_of_elements)
		}
	}

	//db.RunCompaction()

	for level := 1; level <= lsmtree.Findlevel() && level < int(config.Configuration.LSMTreeLevels); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		fmt.Println("Nivo", level)
		tables, err := filepath.Glob("level-" + fmt.Sprintf("%02d", level) + "-usertable-*-Data.db") //Izdvajanje svih tabela trenutnog nivoa, za kompaktovanje
		if err != nil {
			panic(err)
		}

		for _, table := range tables {
			summaryIter := sstable.GetSummaryIteratorFromSSTableFile(table)
			fmt.Println(summaryIter.Begin_key, " ", summaryIter.End_key)
			summaryIter.Close()
		}
	}

}
