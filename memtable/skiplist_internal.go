package memtable

import (
	skiplist "github.com/MomirMilutinovic/NAiSP_Projekat/SkipList"
)

const (
	MAX_SKIP_LIST_HEIGHT = 5
)

// Implementacija interfejsa MemTableInternal putem hes mape
type SkipListInternal struct {
	data *skiplist.SkipList
}

func MakeSkipListInternal() *SkipListInternal {
	return &SkipListInternal{data: skiplist.NewSkipList(MAX_SKIP_LIST_HEIGHT)}
}

// Dobavlja MemTableEntry koji odgovara datom klucu i vraca true
// ili vraca nil i false ako element sa tim kljucem ne postoji
func (skiplistInternal *SkipListInternal) Get(key string) (*MemTableEntry, bool) {
	v := skiplistInternal.data.Search(key)
	if v == nil {
		return nil, false
	} else {
		return MemTableEntryFromBytes(v), true
	}
}

// Dodaje ili menja element u strukturi
// Ako element postoji postavlja value polje odgovarajuceg MemTableEntry-a na value
// Ako element ne postoji, konstruise novi MemTableEntry i dodaje ga u strukturu
func (skiplistInternal *SkipListInternal) Update(key string, value []byte) {
	newEntry := CreateEntry([]byte(key), value)

	updateSuccesful := skiplistInternal.data.Update(key, MemTableEntryToBytes(newEntry))
	if !updateSuccesful {
		skiplistInternal.data.Insert(key, MemTableEntryToBytes(newEntry))
	}
}

// Dobavlja sve elemente iz strukture i vraca ih sortirane po kljucu u rastucem poretku
func (skiplistInternal *SkipListInternal) GetSortedEntries() []*MemTableEntry {
	entries := make([]*MemTableEntry, 0, skiplistInternal.Size())

	// Ubacujemo elemente iz mape u niz koji cemo vratiti
	for _, value := range skiplistInternal.data.FirstLevelValues() {
		entries = append(entries, MemTableEntryFromBytes(value))
	}

	return entries
}

// Logicki brise element iz strukture time sto postavlja tombstone na true
// ako taj element postoji i vraca true. Takodje se vrednost postavlja na
// prazan niz.
// Ako postoji element sa tim kljucem i tombstone-om postavljenim na true, ne radi nista i vraca false.
// Ako ne postoji dodaje MemTable entry sa prosledjenim kljucem, praznim nizom
// kao vrednoscu i tombstone-om postavljenim na true.
func (skiplistInternal *SkipListInternal) Delete(key string) bool {
	v := skiplistInternal.data.Search(key)

	if v != nil {
		entry := MemTableEntryFromBytes(v)
		if entry != nil {
			if entry.Tombstone {
				return false
			}

			entry.Tombstone = true
			entry.Value = []byte{}
			skiplistInternal.data.Update(key, MemTableEntryToBytes(entry))
			return true
		} else {
			// Greska u konverzija MemTableEntry-Byte
			return false
		}
	} else {
		entry := CreateEntry([]byte(key), []byte{})
		entry.Tombstone = true
		skiplistInternal.data.Insert(key, MemTableEntryToBytes(entry))
		return true
	}
}

func (skiplistInternal *SkipListInternal) Clear() {
	skiplistInternal.data = skiplist.NewSkipList(MAX_SKIP_LIST_HEIGHT)
}

func (skiplistInternal *SkipListInternal) Size() int {
	return skiplistInternal.data.Size
}
