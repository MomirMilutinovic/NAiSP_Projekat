package lsmtree

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
	"github.com/MomirMilutinovic/NAiSP_Projekat/memtable"
	"github.com/MomirMilutinovic/NAiSP_Projekat/sstable"
)

// Pravi LSMTree sa memtabelom,maksimalnim nivoom i trenutnim najvecim nivoom
type LogStructuredMergeTree struct {
	memtable     memtable.MemTable
	level        int
	currentlevel int
}

func NewLogStructuredMergeTree(capacity int) *LogStructuredMergeTree {
	return &LogStructuredMergeTree{
		memtable:     *memtable.MakeHashMapMemTable(capacity),
		level:        int(config.Configuration.LSMTreeLevels),
		currentlevel: Findlevel(),
	}

}

// Trazi trenutni najveci nivo ili vraca 0 ako nema sstabeli
func Findlevel() int {
	files, err := filepath.Glob("level-*-Data.db")
	if err != nil || len(files) == 0 {
		return 0 // Nema sstabeli, najveci nivo je C0
	}

	last_table_name := files[len(files)-1] // Niz imena fajlova koji ce se vratiti ce biti sortiran
	prefix_before_level := "level-"
	// Definisano je da se koriste dve cifre za nivo
	level, err := strconv.Atoi(last_table_name[len(prefix_before_level) : len(prefix_before_level)+2])

	if err == nil {
		return level
	} else {
		return 0
	}
}

// Trazi u memtable, ako ne nadje trazi u ss tabelama
func (lsmt *LogStructuredMergeTree) Get(key []byte) (string, bool) { //trazi prvo u memtable, ako nije tamo prolazi kroz svaki sstable

	if value, ok := lsmt.memtable.Get(string(key)); ok {
		return string(value), true
	} else {
		return lsmt.FindInSSTable(key)

	}
}

// Trazi kljuc u svim SS tabelama
func (lsmt *LogStructuredMergeTree) FindInSSTable(key []byte) (string, bool) {
	i := 1
	for i <= lsmt.currentlevel {
		levelstr := ""
		if i < 10 {
			levelstr = "0" + fmt.Sprint(i)
		} else {
			levelstr = fmt.Sprint(i)
		}
		if i == 1 {
			entrystr := "level-" + levelstr + "-usertable-000001"
			entry := sstable.ReadOneSSTEntryWithKey(key, entrystr+"--Data.db", entrystr+"--Index.db", entrystr+"--Summary.db", entrystr+"--Filter.db")
			if entry == nil {
			} else {
				return string(entry.Value), true
			}
		} else {
			for j := 1; j < 3; j++ {
				entrystr := "level-" + levelstr + "-usertable-00000" + strconv.Itoa(j)
				entry := sstable.ReadOneSSTEntryWithKey(key, entrystr+"--Data.db", entrystr+"--Index.db", entrystr+"--Summary.db", entrystr+"--Filter.db")
				if entry == nil {
				} else {
					return string(entry.Value), true
				}
			}
		}

	}
	fmt.Println("Ne nalazi se ni u jednoj sstabeli niti memtable")
	return "", false

}

func MergeMultipleTables(files []string, outputfile string) bool {
	iterators := []sstable.SSTableIterator{}
	entries := []sstable.SSTableEntry{}
	writer := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
	writer.Open(outputfile)
	if !writer.Ok {
		panic("Greska pri otvaranju writer-a.")
	}

	for i := range files {
		iterator := sstable.GetSSTableIterator(files[i])
		if iterator == nil {
			fmt.Println("Ne radi")
			continue
		}
		iterators = append(iterators, *iterator)
		entry := iterator.Next()
		if entry == nil {
			fmt.Println("Ne radi")
			continue
		}
		entries = append(entries, *entry)
	}
	for {
		min := &sstable.SSTableEntry{}
		for i := range entries {
			if i == 0 {
				min = &entries[0]
			} else {
				if string(min.Key) > string(entries[i].Key) {
					min = &entries[i]
				} else {

					if string(min.Key) == string(entries[i].Key) {

						if min.Timestamp < entries[i].Timestamp {
							min = &entries[i]
						}
					}

				}
			}
		}
		writer.Put(min)
		if !writer.Ok {
			panic("Greska pri upisu u SSTable.")
		}

		tempkey := string(min.Key)
		iteratorstemp := []sstable.SSTableIterator{}
		entriestemp := []sstable.SSTableEntry{}
		for i := range entries {
			if tempkey == string(entries[i].Key) {
				entry := iterators[i].Next()
				if entry != nil {
					iteratorstemp = append(iteratorstemp, iterators[i])
					entriestemp = append(entriestemp, *entry)
				}
			} else {
				iteratorstemp = append(iteratorstemp, iterators[i])
				entriestemp = append(entriestemp, entries[i])
			}
		}
		if len(iteratorstemp) == 0 {
			writer.Finish()
			break
		} else {
			iterators = iteratorstemp
			entries = entriestemp
		}

	}
	return true
}

func MergeMultipleTablesLCS(files []string, level int) bool {
	iterators := []sstable.SSTableIterator{}
	entries := []sstable.SSTableEntry{}
	writer := sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
	writer.Open("level-" + fmt.Sprintf("%02d", level) + "-usertable-" + fmt.Sprintf("%020d", time.Now().UnixNano()))
	if !writer.Ok {
		panic("Greska pri otvaranju writer-a.")
	}

	for i := range files {
		iterator := sstable.GetSSTableIterator(files[i])
		if iterator == nil {
			fmt.Println("Ne radi")
			continue
		}
		iterators = append(iterators, *iterator)
		entry := iterator.Next()
		if entry == nil {
			fmt.Println("Ne radi")
			continue
		}
		entries = append(entries, *entry)
	}
	for {
		min := &sstable.SSTableEntry{}
		for i := range entries {
			if i == 0 {
				min = &entries[0]
			} else {
				if string(min.Key) > string(entries[i].Key) {
					min = &entries[i]
				} else {

					if string(min.Key) == string(entries[i].Key) {

						if min.Timestamp < entries[i].Timestamp {
							min = &entries[i]
						}
					}

				}
			}
		}
		writer.Put(min)
		if !writer.Ok {
			panic("Greska pri upisivanju u SSTable.")
		}

		tempkey := string(min.Key)
		iteratorstemp := []sstable.SSTableIterator{}
		entriestemp := []sstable.SSTableEntry{}
		for i := range entries {
			if tempkey == string(entries[i].Key) {
				entry := iterators[i].Next()
				if entry != nil {
					iteratorstemp = append(iteratorstemp, iterators[i])
					entriestemp = append(entriestemp, *entry)
				}
			} else {
				iteratorstemp = append(iteratorstemp, iterators[i])
				entriestemp = append(entriestemp, entries[i])
			}
		}
		if len(iteratorstemp) == 0 {
			writer.Finish()
			break
		} else {
			iterators = iteratorstemp
			entries = entriestemp
		}
		if writer.Records_written == 160 { //Ako je zapisano 160 slogova, trenutna tabela se kompletira, a zatim se otvara nova
			writer.Finish()
			if !writer.Ok {
				panic("Greska pri zatvaranju writer-a.")
			}

			writer = sstable.GetSSTFileWriter(config.Configuration.MultipleFileSSTable)
			writer.Open("level-" + fmt.Sprintf("%02d", level) + "-usertable-" + fmt.Sprintf("%020d", time.Now().UnixNano()))
			if !writer.Ok {
				panic("Greska pri otvaranju writer-a.")
			}
		}

	}
	return true
}
