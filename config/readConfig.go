package config

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var Configuration Config = Config{
	WalSize:             10000000,
	MemtableSize:        4,
	MemtableStructure:   "skip_list",
	LSMTreeLevels:       4,
	MultipleFileSSTable: true,
	SummaryDensity:      4,
	CacheSize:           4,
	RateLimit:           999999, // Broj zahteva po sekundi
	CompactionStrategy:  "size_tiered",
	ChunkLength:         64000,
}

var DefaultConfiguration Config = Config{
	WalSize:             10000000,
	MemtableSize:        4,
	MemtableStructure:   "b_tree",
	LSMTreeLevels:       4,
	MultipleFileSSTable: true,
	SummaryDensity:      4,
	CacheSize:           4,
	RateLimit:           999999,
	CompactionStrategy:  "size_tiered",
	ChunkLength:         64000,
}

type Config struct {
	WalSize             uint64 `yaml:"wal_size"`               // Velicina WAL segmenta u bajtovima
	MemtableSize        uint64 `yaml:"memtable_size"`          // Max broj zapisa u MemTable (kad dodje do tog broja flush-uje se)
	MemtableStructure   string `yaml:"memtable_structure"`     // Koja struktura se koristi za memtable ("hashmap", "skip_list", "btree")
	LSMTreeLevels       uint64 `yaml:"lsm_tree_levels"`        // Maksimalan broj nivoa LSM stabla
	MultipleFileSSTable bool   `yaml:"sstable_multiple_files"` // Da li se SSTable pise kao vise fajlova
	SummaryDensity      uint64 `yaml:"summary_density"`        // Koliko zapisa pokriva jedan zapis u summary-u
	CacheSize           uint64 `yaml:"cache_size"`             // Broj elemenata u kesu
	RateLimit           uint64 `yaml:"rate_limit"`             // Rate limit za token bucket
	CompactionStrategy  string `yaml:"compaction_strategy"`
	ChunkLength         uint64 `yaml:"chunk_length"` // Kolicina podataka u bajtovima koja se smesta u jedan kompresovani blok
}

func ReadConfig() {
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		fmt.Println("Nije se ucitala konfiguracija")
		Configuration = DefaultConfiguration
		return
	}
	err = yaml.Unmarshal(configData, &Configuration)
	fmt.Println(Configuration.MemtableSize)

	memTableStructureValid := Configuration.MemtableStructure == "hashmap" ||
		Configuration.MemtableStructure == "skip_list" ||
		Configuration.MemtableStructure == "b_tree"

	compactionStrategyValid := Configuration.CompactionStrategy == "size_tiered" || Configuration.CompactionStrategy == "leveled"

	lsmTreeLevelsValid := Configuration.LSMTreeLevels >= 1
	summaryDensityValid := Configuration.SummaryDensity >= 2
	cacheSizeValid := Configuration.CacheSize >= 1
	memTableSizeValid := Configuration.MemtableSize >= 1
	walSizeValid := Configuration.WalSize >= 1000

	if err != nil || !memTableStructureValid || !compactionStrategyValid || !lsmTreeLevelsValid || !summaryDensityValid || !cacheSizeValid || !memTableSizeValid || !walSizeValid {
		// Ako se desi greska u citanju, koristimo default
		fmt.Println("Nije se ucitala konfiguracija")
		Configuration = DefaultConfiguration
	}
	//fmt.Println(config)
}

// Procitacemo konfiguracioni fajl cim importujemo ovaj paket tj.
// na pocetku programa
func init() {
	ReadConfig()
}
