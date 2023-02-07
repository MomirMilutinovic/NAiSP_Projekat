package engine

import (
	"fmt"
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestCMS(t *testing.T) {
	config.DefaultConfiguration.MemtableSize = 2 // Da se bloom filter odmah flushuje na disk
	config.DefaultConfiguration.RateLimit = 9999
	config.DefaultConfiguration.MultipleFileSSTable = true
	config.ReadConfig()
	Cleanup()
	db := GetNewDB()
	db.CreateCMS(fmt.Sprintf("%03d", 1), 0.9, 0.9)
	db.CreateCMS(fmt.Sprintf("%03d", 2), 0.9, 0.9)

	db.AddToCMS(fmt.Sprintf("%03d", 3), []byte{1})
	db.AddToCMS(fmt.Sprintf("%03d", 1), []byte{1})
	db.AddToCMS(fmt.Sprintf("%03d", 1), []byte{1})
	fmt.Print(db.DeleteCMS(fmt.Sprintf("%03d", 1)))
	fmt.Print(db.ReadFromCMS(fmt.Sprintf("%03d", 1), []byte{1}))

}
