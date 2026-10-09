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
	return &Store{tokenBuckets: make(map[string]*TokenBucket), lastUsed: make(map[string]time.Time), capacity: capacity, refillRate: refillRate, mu: sync.Mutex{}, nowFunc: time.Now}
}

func (s *Store) Allow(key string, n int) Result {
	if n <= 0 {
		return Result{false, false, time.Unix(1<<63-1, 0)}
	}
	s.mu.Lock()
	if s.tokenBuckets[key] == nil {
		s.tokenBuckets[key] = newTokenBucket(s.capacity, s.refillRate, s.nowFunc)
	}
	tokenBucket := s.tokenBuckets[key]
	s.lastUsed[key] = s.nowFunc()
	s.mu.Unlock()
	result := tokenBucket.Allow(n)
	return result
}

func (s *Store) StartCleanup() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleteAfterSeconds := (float64(s.capacity) / s.refillRate) * 1000000000
	deleteAfterTime := s.nowFunc().Add(time.Duration(deleteAfterSeconds) * time.Second)
	for k, t := range s.lastUsed {
		if t.Compare(deleteAfterTime) < 0 {
			delete(s.lastUsed, k)
			delete(s.tokenBuckets, k)
		}
	}
	return true
}
