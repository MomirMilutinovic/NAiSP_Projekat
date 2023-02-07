package engine

import (
	"github.com/MomirMilutinovic/NAiSP_Projekat/BTree"
	"github.com/MomirMilutinovic/NAiSP_Projekat/memtable"
	"github.com/MomirMilutinovic/NAiSP_Projekat/sstable"
)

// Struktura koja agregira rezultate operacija range scan i list nad vise
// sstabela u jedan rezultat i sortira taj rezultat po kljucu.
// Resava problem ove dve operacije da ce se vise verzija vrednosti za isti
// kljuc pojaviti u rezultatima.

type RangeQueryResultAggregator struct {
	results BTree.BTree // Kljuc: string Vrednost: Serijalizovan MemTableEntry
	// Memtable entry se koristi kao vrednost jer sadrzi tacno one
	// podatke koji su relevantni za ovaj agregator: kljuc, vrednost, timestamp i tombstone
}

// Funkcija vraca instancu RangeQueryResultAggregator-a spremnog za upotrebu
func CreateAggregator() *RangeQueryResultAggregator {
	aggregator_btree := BTree.BTree{}
	aggregator_btree.Init(3)
	new_aggregator := RangeQueryResultAggregator{results: aggregator_btree}
	return &new_aggregator
}

// Dodaje zapis u rezultat ili menja postojeci ako je on noviji od njega
func (aggregator *RangeQueryResultAggregator) Add(new_result_entry *memtable.MemTableEntry) {
	found, entry_bytes := aggregator.results.GetValue(new_result_entry.Key)

	if found == 0 {
		entry_already_in_results := memtable.MemTableEntryFromBytes(entry_bytes)
		if new_result_entry.Timestamp > entry_already_in_results.Timestamp {
			aggregator.results.ModifyKey(new_result_entry.Key, memtable.MemTableEntryToBytes(new_result_entry))
		}
	} else {
		aggregator.results.AddKey(new_result_entry.Key, memtable.MemTableEntryToBytes(new_result_entry))
	}
}

// Add funkcija, ali prima SSTableEntry
func (aggregator *RangeQueryResultAggregator) AddSSTableEntry(new_result_entry *sstable.SSTableEntry) {
	memtable_entry := memtable.MemTableEntry{Key: new_result_entry.Key, Value: new_result_entry.Value, Timestamp: new_result_entry.Timestamp, Tombstone: new_result_entry.Tombstone}
	aggregator.Add(&memtable_entry)
}

// Vraca rezultate operacije sortirane po kljucu i uklanja obrisane elemente iz rezultata.
// Vrednost koja se vraca je niz nizova bajtova koji predstavlja vrednosti za kljuceve koji
// zadovoljavaju uslov range-query-a
func (aggregator *RangeQueryResultAggregator) GetResults() [][]byte {
	results_with_tombstones := aggregator.results.GetValuesSortedByKey()

	final_results := make([][]byte, 0)

	for _, entry_bytes := range results_with_tombstones {
		entry := memtable.MemTableEntryFromBytes(entry_bytes)
		if !entry.Tombstone {
			final_results = append(final_results, entry.Value)
		}
	}

	return final_results

}
