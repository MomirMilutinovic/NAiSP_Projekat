package engine

import (
	"github.com/MomirMilutinovic/NAiSP_Projekat/LRU_cache"
	"github.com/MomirMilutinovic/NAiSP_Projekat/config"
	"github.com/MomirMilutinovic/NAiSP_Projekat/memtable"
)

type DB struct {
	cache    LRU_cache.Cache
	memtable memtable.MemTable
	//lsm_tree *lsmtree.LogStructuredMergeTree
	wal_enabled           bool
	Rate_limiting_enabled bool
	tbm                   *TokenBucketManager
	token_bucket          *TokenBucket
}

func GetNewDB() *DB {
	config.ReadConfig()

	cache := LRU_cache.Cache{}
	cache.Init(int(config.Configuration.CacheSize))

	db := DB{cache: cache, memtable: *memtable.MakeMemTableFromConfig(), wal_enabled: true}

	db.Rate_limiting_enabled = false

	// Ponavlajmo sve operacije iz WAL-a
	db.CreateWalDirIfDoesNotExist()

	db.disableWALWriting()
	db.ReplayWal()
	db.enableWALWriting()

	/*
		db.tbm = InitializeTokenBucketManager(&db)

		_, ok := db.tbm.GetTokenBucket(int(config.Configuration.RateLimit), USER_ID)

		if !ok {
			token_bucket, _ := db.tbm.NewTokenBucket(USER_ID, int(config.Configuration.RateLimit), int(config.Configuration.RateLimit))
			db.token_bucket = token_bucket
		}
	*/

	db.Rate_limiting_enabled = true
	return &db
}

func (engine *DB) disableWALWriting() {
	engine.wal_enabled = false
}

func (engine *DB) enableWALWriting() {
	engine.wal_enabled = true
}
