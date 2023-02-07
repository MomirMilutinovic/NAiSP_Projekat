package engine

import (
	"path/filepath"
	"strings"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
	"github.com/MomirMilutinovic/NAiSP_Projekat/sstable"
)

func (engine *DB) List(prefix string, page_number uint, page_size uint) [][]byte {

	/*
		if engine.Rate_limiting_enabled {
			if !engine.RateLimitCheck() {
				fmt.Println("Rate limit")
				return nil
			}
		}
	*/

	if page_number < 1 || page_size == 0 {
		return [][]byte{}
	}

	result_aggregator := CreateAggregator()

	// Prvo range-scan-ujemo memtable
	for _, memtable_entry := range engine.memtable.PrefixScanEntries(prefix) {
		result_aggregator.Add(memtable_entry)
	}

	// Trazimo rezultate u sstabelama

	sstable_filenames, err := filepath.Glob("level-*-Data.db")
	if err != nil {
		sstable_filenames = make([]string, 0)
	}

	for _, sstable_filename := range sstable_filenames {
		index_filename := ""
		summary_filename := ""
		filter_filename := ""
		if config.Configuration.MultipleFileSSTable {
			index_filename = strings.Replace(sstable_filename, "-Data.db", "-Index.db", -1)
			summary_filename = strings.Replace(sstable_filename, "-Data.db", "-Summary.db", -1)
			filter_filename = strings.Replace(sstable_filename, "-Data.db", "-Filter.db", -1)
		}

		matchingEntries := sstable.PrefixScanSSTable(prefix, sstable_filename, index_filename, summary_filename, filter_filename)

		for _, entry := range matchingEntries {
			// Moramo proveriti da li je kljuc bio obrisan iz memtabele
			// Ako jeste onda je on trenutno obrisan i ne treba ga vratiti
			result_aggregator.AddSSTableEntry(entry)
		}
	}

	begin_index_to_return := (page_number - 1) * page_size

	values := result_aggregator.GetResults()

	if len(values) < int(begin_index_to_return) {
		return [][]byte{}
	} else {
		return values[begin_index_to_return:min(int(begin_index_to_return)+int(page_size), len(values))]
	}
}
