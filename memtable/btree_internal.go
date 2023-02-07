package memtable

import (
	"fmt"

	"github.com/MomirMilutinovic/NAiSP_Projekat/BTree"
)

// Implementacija interfejsa MemTableInternal putem hes mape
type BTreeInternal struct {
	data BTree.BTree
	size int
}

const (
	B_TREE_NODE_SIZE = 3
)

func MakeBTreeInternal() *BTreeInternal {
	btree := BTree.BTree{}
	btree.Init(B_TREE_NODE_SIZE)
	return &BTreeInternal{data: btree, size: 0}
}

// Dobavlja MemTableEntry koji odgovara datom klucu i vraca true
// ili vraca nil i false ako element sa tim kljucem ne postoji
func (btreeInternal *BTreeInternal) Get(key string) (*MemTableEntry, bool) {
	ok, v := btreeInternal.data.GetValue([]byte(key))
	if ok == -1 {
		return nil, false
	} else {
		return MemTableEntryFromBytes(v), true
	}
}

// Dodaje ili menja element u strukturi
// Ako element postoji postavlja value polje odgovarajuceg MemTableEntry-a na value
// Ako element ne postoji, konstruise novi MemTableEntry i dodaje ga u strukturu
func (btreeInternal *BTreeInternal) Update(key string, value []byte) {
	newEntry := CreateEntry([]byte(key), value)
	newEntryBytes := MemTableEntryToBytes(newEntry)

	if newEntryBytes == nil {
		fmt.Println("Greska u pisanju u memtable (BTree)")
		return
	}

	//updateSuccesful := btreeInternal.data.ModifyKey(newEntry.Key, newEntryBytes)
	searchSuccessful, _ := btreeInternal.data.Search([]byte(key))
	if searchSuccessful == -1 {
		btreeInternal.data.AddKey(newEntry.Key, newEntryBytes)
		btreeInternal.size++
	} else {
		btreeInternal.data.ModifyKey(newEntry.Key, newEntryBytes)
	}
}

// Dobavlja sve elemente iz strukture i vraca ih sortirane po kljucu u rastucem poretku
func (btreeInternal *BTreeInternal) GetSortedEntries() []*MemTableEntry {
	entries := make([]*MemTableEntry, 0, btreeInternal.Size())

	for _, value := range btreeInternal.data.GetValuesSortedByKey() {
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
func (btreeInternal *BTreeInternal) Delete(key string) bool {
	ok, v := btreeInternal.data.GetValue([]byte(key))

	// v := skiplistInternal.data.Search(key)

	if ok != -1 {
		entry := MemTableEntryFromBytes(v)
		if entry != nil {
			if entry.Tombstone {
				return false
			}

			entry.Tombstone = true
			entry.Value = []byte{}
			btreeInternal.data.ModifyKey(entry.Key, MemTableEntryToBytes(entry))
			return true
		} else {
			// Greska u konverzija MemTableEntry-Byte
			return false
		}
	} else {
		entry := CreateEntry([]byte(key), []byte{})
		entry.Tombstone = true
		btreeInternal.data.AddKey([]byte(key), MemTableEntryToBytes(entry))
		btreeInternal.size++ // Dodali smo element u b stablo
		return true
	}
}

func (btreeInternal *BTreeInternal) Clear() {
	// Garbage collector ce ukloniti staro stablo
	btreeInternal.data.Init(B_TREE_NODE_SIZE)
	btreeInternal.size = 0
}

func (btreeInternal *BTreeInternal) Size() int {
	return btreeInternal.size
}
