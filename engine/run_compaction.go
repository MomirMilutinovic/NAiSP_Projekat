package engine

import (
	compactions "github.com/darkokos/NAiSP_Projekat/Compactions"
	"github.com/darkokos/NAiSP_Projekat/config"
)

// Funkcija pokrece odgovarajucu kompakciju na osnovu konfiguracije
func (engine *DB) RunCompaction() {
	if config.Configuration.CompactionStrategy == "size_tiered" {
		compactions.STCS()
	} else if config.Configuration.CompactionStrategy == "leveled" {
		compactions.LCS()
	}
}
