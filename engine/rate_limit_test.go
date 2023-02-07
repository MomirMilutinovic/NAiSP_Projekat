package engine

import (
	"testing"

	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
)

func TestRateLimit(t *testing.T) {
	config.DefaultConfiguration.RateLimit = 3
	config.ReadConfig()
	Cleanup()
	defer Cleanup()

	db := GetNewDB()

	db.Get("123")
	db.Get("123")
	db.Get("123")
	db.Get("123")
	db.Get("123")
	db.Get("123")

}
