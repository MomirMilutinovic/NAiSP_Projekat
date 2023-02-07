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

func TestRateLimitPut(t *testing.T) {
	config.DefaultConfiguration.RateLimit = 5
	config.ReadConfig()
	Cleanup()
	defer Cleanup()

	db := GetNewDB()

	db.Put("123", []byte{})
	db.Put("124", []byte{})
	db.Put("125", []byte{})
	db.Put("126", []byte{})
	db.Put("127", []byte{})
	db.Delete("123")

}
