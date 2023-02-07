package compactions

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	lsmtree "github.com/MomirMilutinovic/NAiSP_Projekat/LSMTree"
	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func STCS() {
	for level := 1; level <= lsmtree.Findlevel() && level < int(config.Configuration.LSMTreeLevels); level++ { //Prolazi se kroz sve nivoe LSM stabla sem poslednjeg, jer se on ne kompaktuje, radi korektnog lancanog kompaktovanja
		tables_to_merge, err := filepath.Glob("level-" + fmt.Sprintf("%02d", level) + "-usertable-*-Data.db") //Izdvajanje svih tabela trenutnog nivoa, za kompaktovanje
		if err != nil {
			panic(err)
		}

		num_of_tables := len(tables_to_merge)

		if num_of_tables < 4 { //Kraj algoritma ako ne postoje barem 2 tabele za kompakciju
			break
		}

		result_table_name := "level-" + fmt.Sprintf("%02d", level+1) + "-usertable-" + fmt.Sprintf("%020d", time.Now().UnixNano()) //Naziv tabele na sledecem nivou koja ce se dobiti kao rezultat kompaktovanja

		lsmtree.MergeMultipleTables(tables_to_merge, result_table_name)

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
