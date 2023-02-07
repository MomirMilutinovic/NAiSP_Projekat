package engine

import (
	"encoding/json"
	"sync"
	"time"
)

type TokenBucket struct {
	Tokens chan struct{}
	Stop   chan struct{}
	UserID string
}

type TokenBucketManager struct {
	db *DB
	mu sync.Mutex
}

func InitializeTokenBucketManager(db *DB) *TokenBucketManager {
	return &TokenBucketManager{
		db: db,
	}
}

func (tb *TokenBucket) toJSON() []byte {
	jsonBytes, _ := json.Marshal(tb)
	return jsonBytes
}

func (tbm *TokenBucketManager) NewTokenBucket(userID string, rate int, capacity int) (*TokenBucket, bool) {
	tb, exists := tbm.GetTokenBucket(rate, userID)
	if exists {
		return tb, true // Token bucket vec postoji za korisnika: userID
	}

	tbm.mu.Lock()
	defer tbm.mu.Unlock()

	tb = &TokenBucket{
		Tokens: make(chan struct{}, capacity),
		Stop:   make(chan struct{}),
		UserID: userID,
	}

	for i := 0; i < capacity; i++ {
		tb.Tokens <- struct{}{}
	}

	go tb.fill(rate, tbm)

	b := tbm.db.PutNoRateLimit(userID, []byte(tb.toJSON()))
	if !b {
		tb.Stop <- struct{}{}
		return nil, false // Neuspesno spremanje token bucketa u bazu podataka"
	}

	return tb, true
}

func (tbm *TokenBucketManager) GetTokenBucket(rate int, userID string) (*TokenBucket, bool) {
	tbm.mu.Lock()
	defer tbm.mu.Unlock()

	data := tbm.db.GetNoRateLimit(userID)
	if data == nil {
		return nil, false // Token bucket ne postoji za korisnika: userID
	}

	tb := &TokenBucket{}
	json.Unmarshal(data, tb)

	go tb.fill(rate, tbm)

	return tb, true
}

func (tb *TokenBucket) fill(rate int, tbm *TokenBucketManager) {
	ticker := time.NewTicker(time.Duration(1e9 / rate))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			select {
			case tb.Tokens <- struct{}{}:
			default:
			}
			tbm.db.PutNoRateLimit(tb.UserID, []byte(tb.toJSON()))
		case <-tb.Stop:
			return
		}
	}
}

func (tb *TokenBucket) Take(tbm *TokenBucketManager) bool {
	select {
	case <-tb.Tokens:
		tbm.mu.Lock()
		defer tbm.mu.Unlock()
		data, _ := json.Marshal(tb)
		tbm.db.PutNoRateLimit(tb.UserID, []byte(data))
		return true
	default:
		return false
	}
}

func (tb *TokenBucket) Close(tbm *TokenBucketManager) (success bool) {
	tb.Stop <- struct{}{}
	return tbm.db.Delete(tb.UserID)
}

func (tb *TokenBucket) GetUserID() string {
	return tb.UserID
}

/*
func TokenBucketTest() {
	tbm := InitializeTokenBucketManager()
	tb1, err := tbm.NewTokenBucket("user1", 1, 5) // 1 token po sekundi, kapacitet max 5 tokena
	if !err {
		fmt.Println("Neuspesno spremanje token bucketa u bazu podataka")
		return
	}
	tb2, err := tbm.NewTokenBucket("user2", 2, 10) // 2 tokena po sekundi, kapacitet max 10 tokena
	if !err {
		fmt.Println("Neuspesno spremanje token bucketa u bazu podataka")
		return
	}
	for i := 0; i < 80; i++ {
		time.Sleep(50 * time.Millisecond)
		//tb1 := tbm.GetTokenBucket("user1")
		//tb2 := tbm.GetTokenBucket("user2")
		fmt.Println("Zahtev", i+1)
		if tb1.Take(tbm) {
			fmt.Println("Token uzet za korisnika:", tb1.GetUserID())
		} else {
			fmt.Printf("Previse zahteva za korisnika: %s, pokusajte opet kasnije.\n", tb1.GetUserID())
		}
		if tb2.Take(tbm) {
			fmt.Println("Token uzet za korisnika:", tb2.GetUserID())
		} else {
			fmt.Printf("Previse zahteva za korisnika: %s, pokusajte opet kasnije.\n", tb2.GetUserID())
		}
	}
	tb1.Close(tbm)
	tb2.Close(tbm)
}
*/
