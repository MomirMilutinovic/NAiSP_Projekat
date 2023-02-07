package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/MomirMilutinovic/NAiSP_Projekat/utils"
)

// Funkcija cita magicni broj iz sst fajla i vraca ga kao povratnu vrednost
// sstFile deskriptor ce biti postavljen na kraj fajla u slucaju uspesnog citanja
// Ako citanje nije uspesno vraca 0
func readMagicNumber(sstFile *os.File) uint64 {

	size := utils.GetFileSize(sstFile.Name())
	if size == -1 {
		return 0
	}

	magic_number_offset := size - SSTABLE_MAGIC_NUMBER_SIZE

	_, err := sstFile.Seek(magic_number_offset, io.SeekStart)
	if err != nil {
		return 0
	}

	magic_number_bytes := make([]byte, 8)
	err = binary.Read(sstFile, binary.LittleEndian, magic_number_bytes)
	if err != nil {
		return 0
	}

	return binary.LittleEndian.Uint64(magic_number_bytes)
}
