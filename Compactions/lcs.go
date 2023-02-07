package compactions

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	lsmtree "github.com/MomirMilutinovic/NAiSP_Projekat/LSMTree"
	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func LCS() {
	for level := 1; level <= lsmtree.Findlevel() && level < int(config.Configuration.LSMTreeLevels); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		tables_to_merge, err := filepath.Glob("level-" + fmt.Sprintf("%02d", level) + "-usertable-*-Data.db") //Izdvajanje svih tabela trenutnog nivoa, za kompaktovanje
		if err != nil {
			panic(err)
		}

		num_of_tables := len(tables_to_merge)

		if num_of_tables < int(math.Pow10(level)) { //Kraj algoritma ako na trenutnom nivou nema 10 * broj tabela prethodnog nivoa
			break
		}

		lsmtree.MergeMultipleTablesLCS(tables_to_merge, level+1) //Poseban algoritam za merge koji ce otvarati novu tabelu svaki put kada se napise 160 zapisa u trenutnu, pocevsi od prosledjene tabele. Potrebno ga je napisati u implementaciji LSM stabla, modifikacijom STCS merge algoritma.

		for _, table := range tables_to_merge { //Brisanje tabela i svih propratnih fajlova nakon kompakcije
			table_prefix := table[:len(table)-8]

			tables_to_remove, err := filepath.Glob(table_prefix + "*")
			if err != nil {
				panic(err)
			}

			for _, table_to_remove := range tables_to_remove {
				os.Remove(table_to_remove)
			}
		}
	}
}
