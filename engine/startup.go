package engine

import (
	"io/ioutil"
	"log"
	"os"

	wal "github.com/MomirMilutinovic/NAiSP_Projekat/WAL"
)

// Cita WAL-ove i ponovo primenjuje operacije
func (engine *DB) ReplayWal() {
	_, err := ioutil.ReadDir("./wal")
	if err != nil {
		log.Println("Nije citanje wal direktorijuma. WAL nije procitan.")
	}

	/*
		for _, file := range files {
			wal.ReadWAL()
		}
	*/

	walEntries := wal.ReadWAL()

	for _, walEntry := range walEntries {
		if !walEntry.Tombstone {
			engine.PutNoRateLimit(string(walEntry.Key), walEntry.Value)
		} else {
			engine.DeleteNoRateLimit(string(walEntry.Key))
		}
	}
}

// Pravi wal direktorijum u trenutnom radnom direktorijumu ako ne postoji
func (engine *DB) CreateWalDirIfDoesNotExist() {
	_, err := os.Stat("wal")
	if os.IsNotExist(err) {
		err := os.Mkdir("wal", os.ModePerm)
		if err != nil {
			log.Println("Nije uspelo kreiranje wal direktorijuma. WAL nije procitan.")
		}
	}
}
