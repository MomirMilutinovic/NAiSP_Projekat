package engine

import (
	"fmt"

	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
)

func (engine *DB) Delete(key string) bool {

	if !engine.RateLimitCheck() {
		fmt.Println("Rate limit")
		return false
	}

	// Belezimo brisanje u WAL
	if engine.wal_enabled {
		walEntry := wal.CreateWALEntry(true, []byte(key), []byte{})
		walEntry.Append()
	}

	if r := recover(); r != nil {
		// Nije uspelo dodavanje u WAL
		return false
	}

	ok := engine.memtable.Delete(key)
	if ok {
		engine.cache.Edit([]byte(key), nil) // Moramo ukloniti element iz kesa - prevencija zastarelog kesa
		if engine.wal_enabled {
			wal.DeleteSegments()
		}
		return true
	} else {
		return false
	}
}

// Delete operacija, ali bez rate-limit-a
func (engine *DB) DeleteNoRateLimit(key string) bool {

	// Belezimo brisanje u WAL
	if engine.wal_enabled {
		walEntry := wal.CreateWALEntry(true, []byte(key), []byte{})
		walEntry.Append()
	}

	if r := recover(); r != nil {
		// Nije uspelo dodavanje u WAL
		return false
	}

	ok := engine.memtable.Delete(key)
	if ok {
		engine.cache.Edit([]byte(key), nil) // Moramo ukloniti element iz kesa - prevencija zastarelog kesa
		if engine.wal_enabled {
			wal.DeleteSegments()
		}
		return true
	} else {
		return false
	}
}
