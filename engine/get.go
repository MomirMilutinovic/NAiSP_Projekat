package engine

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
	"github.com/MomirMilutinovic/NAiSP_Projekat/sstable"
)

// Dobavlja vrednost za prosledjeni kljuc iz baze podataka.
// Vraca vrednost koja je dodelejena tom kljucu ako postoji u bazi ili nil
// ako zapis sa prosledjenim kljucem ne postoji.
func (engine *DB) Get(key string) []byte {

	if !engine.RateLimitCheck() {
		fmt.Println("Rate limit")
		return nil
	}

	key_bytes := []byte(key)
	//val, ok := engine.lsm_tree.Get(key_bytes)

	// Prvo trazimo u memtable-u
	val, ok := engine.memtable.Get(key)

	if engine.memtable.IsDeleted(key) {
		return nil
	}

	if ok {
		engine.cache.Add(key_bytes, val) // Da li treba dodavati u kes ako smo citali iz memtable-a?
		return val
	}

	// Ako je kljuc bio nedavno obrisan onda odmah vracamo tu informaciju
	if engine.memtable.IsDeleted(key) {
		return nil
	}

	// Da li je element u kesu?
	val, ok_cache := engine.cache.Access(key_bytes)

	if ok_cache == 0 {
		return val
	}

	// Citaj SSTabele

	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	var entry_to_return *sstable.SSTableEntry = nil

	// TODO: Kada budu dodate kompakcije promeniti read path jer onda mozemo ranije da se zaustavimo
	// Gledamo svaku SSTabelu
	for _, file := range files {
		filename := file.Name()
		var currently_read_entry *sstable.SSTableEntry = nil
		if strings.HasSuffix(filename, "-Data.db") {
			if config.Configuration.MultipleFileSSTable {
				base_filename := filename[:len(filename)-len("-Data.db")]
				currently_read_entry = sstable.ReadOneSSTEntryWithKey(key_bytes, filename, base_filename+"-Index.db", base_filename+"-Summary.db", base_filename+"-Filter.db")
			} else {
				currently_read_entry = sstable.ReadOneSSTEntryWithKey(key_bytes, file.Name(), "", "", "")
			}
		}

		// Menjamo koji zapis vracamo samo ako je noviji od onog sto imamo
		if currently_read_entry != nil {
			if entry_to_return == nil {
				entry_to_return = currently_read_entry
			} else if entry_to_return.Timestamp < currently_read_entry.Timestamp {
				entry_to_return = currently_read_entry
			}
		}
	}

	if entry_to_return == nil || entry_to_return.Tombstone {
		return nil
	}

	// Vracamo samo entry-e koji nisu obrisani
	if entry_to_return != nil && !entry_to_return.Tombstone {
		engine.cache.Add(entry_to_return.Key, entry_to_return.Value)
		return entry_to_return.Value
	} else {
		return nil
	}
}

// Get operacija, ali bez rate-limita
func (engine *DB) GetNoRateLimit(key string) []byte {

	key_bytes := []byte(key)
	//val, ok := engine.lsm_tree.Get(key_bytes)

	// Prvo trazimo u memtable-u
	val, ok := engine.memtable.Get(key)

	if engine.memtable.IsDeleted(key) {
		return nil
	}

	if ok {
		engine.cache.Add(key_bytes, val) // Da li treba dodavati u kes ako smo citali iz memtable-a?
		return val
	}

	// Ako je kljuc bio nedavno obrisan onda odmah vracamo tu informaciju
	if engine.memtable.IsDeleted(key) {
		return nil
	}

	// Da li je element u kesu?
	val, ok_cache := engine.cache.Access(key_bytes)

	if ok_cache == 0 {
		return val
	}

	// Citaj SSTabele

	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	var entry_to_return *sstable.SSTableEntry = nil

	// TODO: Kada budu dodate kompakcije promeniti read path jer onda mozemo ranije da se zaustavimo
	// Gledamo svaku SSTabelu
	for _, file := range files {
		filename := file.Name()
		var currently_read_entry *sstable.SSTableEntry = nil
		if strings.HasSuffix(filename, "-Data.db") {
			if config.Configuration.MultipleFileSSTable {
				base_filename := filename[:len(filename)-len("-Data.db")]
				currently_read_entry = sstable.ReadOneSSTEntryWithKey(key_bytes, filename, base_filename+"-Index.db", base_filename+"-Summary.db", base_filename+"-Filter.db")
			} else {
				currently_read_entry = sstable.ReadOneSSTEntryWithKey(key_bytes, file.Name(), "", "", "")
			}
		}

		// Menjamo koji zapis vracamo samo ako je noviji od onog sto imamo
		if currently_read_entry != nil {
			if entry_to_return == nil {
				entry_to_return = currently_read_entry
			} else if entry_to_return.Timestamp < currently_read_entry.Timestamp {
				entry_to_return = currently_read_entry
			}
		}
	}

	if entry_to_return == nil || entry_to_return.Tombstone {
		return nil
	}

	// Vracamo samo entry-e koji nisu obrisani
	if entry_to_return != nil && !entry_to_return.Tombstone {
		engine.cache.Add(entry_to_return.Key, entry_to_return.Value)
		return entry_to_return.Value
	} else {
		return nil
	}
}
