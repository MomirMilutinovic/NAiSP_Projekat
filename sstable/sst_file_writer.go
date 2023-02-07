package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	bloomfilter "github.com/MomirMilutinovic/NAiSP_Projekat/bloom-filter"
	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
	"github.com/MomirMilutinovic/NAiSP_Projekat/merkleTree"
)

const (
	SSTABLE_MAGIC_NUMBER_SIZE         = 8                          // Velicina magicnog broja u bajtovima
	SSTABLE_MULTI_FILE_MAGIC_NUMBER   = uint64(0x473700DD14E7F08B) // Magicni broj za SSTabelu u rezimu gde je jedna SSTabele sacinjena iz vise fajlova
	SSTABALE_SINGLE_FILE_MAGIC_NUMBER = uint64(0xE14695378B12D2F8) // Magicni broj za SSTabelu u rezimu gde je su svi elementi SSTabele u jednom fajlu
)

// Sturkutra koja omogucava pisanje SSTable-a zapis po zapis
// Ocekivana upotreba:
// writer := GetSSTableFileWriter(true/false)
// writer.open("level-xx-usertable-xxxxxx")
// writer.Put(zapis) za svaki zapis
// writer.Close()
type SSTFileWriter struct {
	sstFile           *os.File
	summaryFile       *os.File
	indexFile         *os.File
	filterFile        *os.File
	metadataFile      *os.File
	tocFile           *os.File
	is_multiple_files bool
	Records_written   int // Broj zapisa koji su zapisani

	// Vrednosti neophodne za pravilno pisanje summary-a
	next_summary_key    []byte
	next_summary_offset int64
	last_key_written    []byte
	first_key_written   []byte

	valuesWritten [][]byte // Mora zbog merkle stabla - sve vrednosti koje su bile zapisane u SSTabelu
	Ok            bool     // Da li je doslo do greske u pisanju SSTabele
}

// Funkcija konstruise jedan SSTFileWriter
func GetSSTFileWriter(mulitple_files bool) SSTFileWriter {
	return SSTFileWriter{sstFile: nil, summaryFile: nil, indexFile: nil, metadataFile: nil, tocFile: nil, is_multiple_files: mulitple_files, next_summary_key: []byte{}, next_summary_offset: 0, Records_written: 0, last_key_written: []byte{}, Ok: true, valuesWritten: make([][]byte, 0)}
}

// Funkcija otvara fajl(ove) za upis SSTabele
//
// Ako je writer.multiple_files postavljen na true, funkcija otvara 5 fajlova,
// a u suprotnom otvara se samo sstFile, a ostali atributi tipa os.File ce
// ostati nil.
// Ako je doslo do greske, atribut Ok ce biti postavljen na false i bilo koji
// otvoreni fajlovi ce biti zatvoreni.
//
// Parametar base name: String koji ce stojati pre -Data.db/-Index.db/... u imenima fajlova
func (writer *SSTFileWriter) Open(base_name string) {
	if writer.is_multiple_files {
		// Pisanje sstabele kao vise fajlova
		file_open_fail := false
		sstFile, err := os.OpenFile(base_name+"-Data.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		indexFile, err := os.OpenFile(base_name+"-Index.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		summaryFile, err := os.OpenFile(base_name+"-Summary.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		// Pisemo bajtove koji ce predstavljati pokazivac ka footer-u summary-a
		err = binary.Write(summaryFile, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		if err != nil {
			file_open_fail = true
		}

		filterFile, err := os.OpenFile(base_name+"-Filter.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		metadataFile, err := os.OpenFile(base_name+"-Metadata.txt", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		tocFile, err := os.OpenFile(base_name+"-TOC.txt", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		if file_open_fail {
			writer.Ok = false
			sstFile.Close()
			indexFile.Close()
			summaryFile.Close()
			filterFile.Close()
			metadataFile.Close()
			tocFile.Close()
			return
		} else {
			writer.sstFile = sstFile
			writer.indexFile = indexFile
			writer.summaryFile = summaryFile
			writer.filterFile = filterFile
			writer.metadataFile = metadataFile
			writer.tocFile = tocFile
		}

	} else {
		// Pisanje sstabele kao jedan fajl
		file_open_fail := false
		sstFile, err := os.OpenFile(base_name+"-Data.db", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			file_open_fail = true
		}

		if file_open_fail {
			writer.Ok = false
			sstFile.Close()
			return
		} else {
			writer.sstFile = sstFile
		}
	}
}

// Funkcija upisuje jedan SSTableEntry u SSTable
// U SSTable se upisuju entry-i redom koji su prosledjivani kroz ovu funkciju. Ne vrsi se sortiranje.
// Ako je writer u rezimu pisanja u vise fajlova, dodavace i zapise u index i summary
// Ako dodje do greske atribut Ok ce biti postavljen na false
func (writer *SSTFileWriter) Put(entry *SSTableEntry) {
	summary_density := int(config.Configuration.SummaryDensity)

	key := entry.Key
	//value := entry.Value
	offset, err := writer.sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		// handle error
		panic(err)
	}

	//TODO: Proveri da li je doslo do greske pri pisanju reda SSTabele
	writeSSTableEntry(writer.sstFile, entry)

	if writer.Records_written == 0 {
		writer.first_key_written = entry.Key
	}

	writer.valuesWritten = append(writer.valuesWritten, entry.Value)
	writer.last_key_written = entry.Key

	if writer.is_multiple_files {
		if writer.Records_written%summary_density == 0 {
			writer.next_summary_key = entry.Key
		}

		writeIndexEntry(writer.indexFile, string(key), uint64(offset))
		//fmt.Println("Kljuc: ", key, "Vrednost: ", value)

		writer.Records_written++
		if writer.Records_written%summary_density == 0 {
			writeSummaryEntry(writer.summaryFile, writer.next_summary_key, entry.Key, writer.next_summary_offset)
			writer.next_summary_offset, err = writer.indexFile.Seek(0, io.SeekCurrent)
			if err != nil {
				// handle error
				panic(err)
			}
		}
	} else {
		writer.Records_written++
	}
}

// Zatvara fajlove koje je writer otvorio
func (writer *SSTFileWriter) CloseFiles() {
	writer.sstFile.Close()
	if writer.is_multiple_files {
		writer.indexFile.Close()
		writer.summaryFile.Close()
		writer.filterFile.Close()
		writer.metadataFile.Close()
		writer.tocFile.Close()
	}
}

// Kompletira proces pravljenja SSTabele
// Pise pomocne strukture na odgovarajuca mesta i zatvara fajlove.
// Ako dodje do greske atribut Ok ce biti postavljen na false.
func (writer *SSTFileWriter) Finish() {
	if writer.Records_written == 0 {
		writer.CloseFiles()
		return
	}
	summary_density := int(config.Configuration.SummaryDensity)

	endOfData, err := writer.sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		writer.Ok = false
		writer.CloseFiles()
		return
	}

	//Vracamo se na pocetak SSTabele
	sstFileReadOnly, err := os.Open(writer.sstFile.Name())
	if err != nil {
		writer.Ok = false
		writer.CloseFiles()
		return
	}

	sstIter := SSTableIterator{sstFile: sstFileReadOnly, end_offset: endOfData, Valid: true, Ok: true}

	filter := bloomfilter.CreateBloomFilterBasedOnParams(writer.Records_written, FALSE_POSITIVE_RATE)

	metadata := merkleTree.CreateMerkleTree(writer.valuesWritten)
	metadataBytes := merkleTree.SerializeTree(metadata)

	if writer.is_multiple_files {
		for entry := sstIter.Next(); sstIter.Valid; entry = sstIter.Next() {
			filter.Add(entry.Key)
		}

		serialized_filter := filter.Serialize()
		serialized_length := uint64(len(serialized_filter))

		if writer.Records_written%summary_density != 0 {
			writeSummaryEntry(writer.summaryFile, writer.next_summary_key, writer.last_key_written, writer.next_summary_offset)
		}

		summaryFooterOffset, err := writer.summaryFile.Seek(0, io.SeekCurrent)
		if err != nil {
			// handle error
			panic(err)
		}

		writeSummaryHeader(writer.summaryFile, writer.first_key_written, writer.last_key_written)

		_, err = writer.summaryFile.Seek(0, io.SeekStart)
		if err != nil {
			// handle error
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Pisanje offseta za footer od summary-a
		err = binary.Write(writer.summaryFile, binary.LittleEndian, uint64(summaryFooterOffset))
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Zapisivanje duzine bloom filtera
		err = binary.Write(writer.filterFile, binary.LittleEndian, serialized_length)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles() // TODO: CloseFiles se moze defer-ovati
			return
		}

		// Zapisivanje bloom filtera
		binary.Write(writer.filterFile, binary.LittleEndian, serialized_filter)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		//Zapisivanje Merkle Stala
		err = binary.Write(writer.metadataFile, binary.LittleEndian, metadataBytes)
		if err != nil {
			fmt.Println("Greska u zapsivanju merkle stabla")
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Zapsivanje TOC-a
		toc_contents := writer.sstFile.Name() + "\n" + writer.indexFile.Name() +
			"\n" + writer.summaryFile.Name() + "\n" + writer.filterFile.Name() + "\n" +
			writer.metadataFile.Name() + "\n" + writer.tocFile.Name()

		_, err = writer.tocFile.Write([]byte(toc_contents))
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Magicni broj
		err = binary.Write(writer.sstFile, binary.LittleEndian, SSTABLE_MULTI_FILE_MAGIC_NUMBER)
		if err != nil {
			panic(err)
		}
	} else {
		summary_keys := make([][]byte, 0, writer.Records_written)
		index_offsets := make([]int64, 0, writer.Records_written)
		records_ingested := 0

		currentIndexOffset := endOfData

		indexOffset := currentIndexOffset

		// Kreiranje bloom filtera i pisanje indeksa
		currentOffset := sstIter.Tell()
		if currentOffset == -1 {
			writer.CloseFiles()
			writer.Ok = false
			return
		}
		for entry := sstIter.Next(); sstIter.Valid; entry = sstIter.Next() {
			if records_ingested%summary_density == 0 {
				summary_keys = append(summary_keys, entry.Key)
			}

			filter.Add(entry.Key)

			writeIndexEntry(writer.sstFile, string(entry.Key), uint64(currentOffset))

			records_ingested++

			if records_ingested%summary_density == 0 {
				summary_keys = append(summary_keys, entry.Key)
				index_offsets = append(index_offsets, currentIndexOffset)
				currentIndexOffset, err = writer.sstFile.Seek(0, io.SeekCurrent)
				if err != nil {
					writer.Ok = false
					writer.CloseFiles()
					return
				}
			}

			currentOffset = sstIter.Tell()
			if currentOffset == -1 {
				writer.CloseFiles()
				writer.Ok = false
				return
			}
		}

		serialized_filter := filter.Serialize()
		serialized_length := uint64(len(serialized_filter))

		if records_ingested%summary_density != 0 {
			summary_keys = append(summary_keys, writer.last_key_written)
			index_offsets = append(index_offsets, currentIndexOffset)
		}

		summaryOffset, err := writer.sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Pisemo bajtove koji ce predstavljati pokazivac ka footer-u summary-a
		err = binary.Write(writer.sstFile, binary.LittleEndian, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		//Zapsivanje summary-a
		for i := 0; i < len(index_offsets); i++ {
			writeSummaryEntry(writer.sstFile, summary_keys[2*i], summary_keys[2*i+1], index_offsets[i])
		}

		summaryFooterOffset, err := writer.sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		_, err = writer.sstFile.Seek(summaryOffset, io.SeekStart)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		binary.Write(writer.sstFile, binary.LittleEndian, summaryFooterOffset)

		_, err = writer.sstFile.Seek(summaryFooterOffset, io.SeekStart)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		writeSummaryHeader(writer.sstFile, writer.first_key_written, writer.last_key_written)

		filterOffset, err := writer.sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Zapisivanje duzine bloom filtera
		err = binary.Write(writer.sstFile, binary.LittleEndian, serialized_length)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Zapisivanje bloom filtera
		binary.Write(writer.sstFile, binary.LittleEndian, serialized_filter)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		metadataOffset, err := writer.sstFile.Seek(0, io.SeekCurrent)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		err = binary.Write(writer.sstFile, binary.LittleEndian, metadataBytes)
		if err != nil {
			fmt.Println("Greska u zapsivanju merkle stabla")
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		footer := SSTFooter{
			IndexOffset:    indexOffset,
			SummaryOffset:  summaryOffset,
			FilterOffset:   filterOffset,
			MetadataOffset: metadataOffset,
		}

		err = binary.Write(writer.sstFile, binary.LittleEndian, footer)
		if err != nil {
			fmt.Println("Greska u zapsivanju footer-a")
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		// Magicni broj
		err = binary.Write(writer.sstFile, binary.LittleEndian, SSTABALE_SINGLE_FILE_MAGIC_NUMBER)
		if err != nil {
			writer.Ok = false
			writer.CloseFiles()
			return
		}

		//Zapisi indeks

		// Mozemo cuvati kljuceve koji idu u summary, nece ih biti puno
		// Zapisi summary
		// Zapisi bloom filter
		// Zapisis merkle stablo

		// Prati gde su offseti svih ovih stvari: kraj Data/pocetak Index, kraj Index/pocetak Summary, kraj Summary/pocetak Filter, kraj Filter/pocetak Metadata

	}
	writer.CloseFiles()
}
