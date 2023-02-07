package engine

import (
	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
)

func (engine *DB) Put(key string, value []byte) bool {
	/*
		if engine.Rate_limiting_enabled {
			if !engine.RateLimitCheck() {
				fmt.Println("Rate limit")
				return false
			}
		}
	*/

	key_bytes := []byte(key)

	if engine.wal_enabled {
		entry := wal.CreateWALEntry(false, key_bytes, value)
		entry.Append()
	}
	if r := recover(); r != nil {
		// Nije uspelo dodavanje u WAL
		return false
	} else {
		engine.cache.Edit(key_bytes, value) // Prevencija zastarelog kesa
		if engine.wal_enabled {
			wal.DeleteSegments()
		}
		return engine.memtable.Update(key, value)
	}

}
