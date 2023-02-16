package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

const (
	SST_FOOTER_SIZE = 5 * 8
)

// Struktura koja predstavlja Footer SSTabele koja se pise kao jedan fajl
type SSTFooter struct {
	IndexOffset           int64 // Pozicija na kojoj pocinje indeks
	SummaryOffset         int64 // Pozicija na kojoj pocinje summary
	FilterOffset          int64 // Pozicija na kojoj pocinje filter
	MetadataOffset        int64 // Pozicija na kojoj pocinje metadata
	CompressionInfoOffset int64 // Pozicija na kojoj pocinje CompressionInfo
	// Napomena: metadata se zavrsava na <velicina fajla> - SSTABLE_MAGIC_NUMBER_SIZE - SST_FOOTER_SIZE
}

// Cita footer iz sst fajla ili vraca nil ako je doslo do greske
// Deskriptor fajla ce biti postavljen na poziciju iza footer-a ako je citanje uspesno
func ReadSSTFooter(f *os.File) *SSTFooter {
	footer := &SSTFooter{}
	_, err := f.Seek((SST_FOOTER_SIZE+SSTABLE_MAGIC_NUMBER_SIZE)*-1, io.SeekEnd)
	if err != nil {
		return nil
	}

	err = binary.Read(f, binary.LittleEndian, footer)
	if err != nil {
		return nil
	}

	return footer
}
