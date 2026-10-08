package limiter

import (
	"sync"
	"time"
)

type Store struct {
	tokenBuckets map[string]*TokenBucket
	lastUsed     map[string]time.Time
	capacity     int
	refillRate   float64
	mu           sync.Mutex
	nowFunc      func() time.Time
}

func NewStore(capacity int, refillRate float64) *Store {
	return &Store{make(map[string]*TokenBucket), make(map[string]time.Time), capacity, refillRate, sync.Mutex{}, time.Now}
}

func (s *Store) Allow(key string, n int) Result {
	s.mu.Lock()
	if s.tokenBuckets[key] == nil {
		s.tokenBuckets[key] = NewTokenBucket(s.capacity, s.refillRate)
	}
	tokenBucket := s.tokenBuckets[key]
	s.lastUsed[key] = s.nowFunc()
	s.mu.Unlock()
	result := tokenBucket.Allow(n)
	return result
}
