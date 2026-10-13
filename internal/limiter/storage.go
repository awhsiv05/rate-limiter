package limiter

import (
	"errors"
	"sync"
	"time"
)

var ErrRefillRateNonPositive = errors.New("refill rate must be positive")
var ErrInvalidInterval = errors.New("invalid interval")
var ErrInvalidTTL = errors.New("invalid ttl")
var ErrCapacityNonPositive = errors.New("capacity must be positive")
var ErrTTLNonPositive = errors.New("ttl must be positive")

type Store struct {
	tokenBuckets map[string]*TokenBucket
	lastUsed     map[string]time.Time
	capacity     int
	refillRate   float64
	mu           sync.Mutex
	timeToLive   time.Duration
	nowFunc      func() time.Time
}

func NewStore(capacity int, refillRate float64, timeToLive time.Duration) (*Store, error) {
	if capacity <= 0 {
		return nil, ErrCapacityNonPositive
	}
	if refillRate <= 0 {
		return nil, ErrRefillRateNonPositive
	}
	if timeToLive <= 0 {
		return nil, ErrTTLNonPositive
	}
	return newStore(capacity, refillRate, time.Now, timeToLive)
}
func newStore(capacity int, refillRate float64, nowFunc func() time.Time, timeToLive time.Duration) (*Store, error) {
	var expectedTTL = time.Duration((float64(capacity) / refillRate) * seconds)

	if timeToLive < expectedTTL {
		return nil, ErrInvalidTTL
	}
	return &Store{tokenBuckets: make(map[string]*TokenBucket),
		lastUsed:   make(map[string]time.Time),
		capacity:   capacity,
		refillRate: refillRate,
		mu:         sync.Mutex{},
		nowFunc:    nowFunc,
		timeToLive: timeToLive}, nil
}

func (s *Store) Allow(key string, n int) (Result, error) {
	if n <= 0 {
		return Result{}, ErrInvalidTokensNonPositive
	}
	if n > s.capacity {
		return Result{}, ErrInvalidTokensExcessive
	}
	s.mu.Lock()
	if s.tokenBuckets[key] == nil {
		s.tokenBuckets[key] = newTokenBucket(s.capacity, s.refillRate, s.nowFunc)
	}
	tokenBucket := s.tokenBuckets[key]
	s.lastUsed[key] = s.nowFunc()
	s.mu.Unlock()

	return tokenBucket.Allow(n)
}

func (s *Store) StartCleanup(interval time.Duration) (func(), error) {
	if interval <= 0 {
		return nil, ErrInvalidInterval
	}
	var stopCh = make(chan struct{})
	var done = make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.evictorIdle(s.nowFunc())
			case <-stopCh:
				return
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(stopCh)
		})
		<-done
	}, nil
}

func (s *Store) evictorIdle(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cnt := 0
	for k, t := range s.lastUsed {
		if t.Add(s.timeToLive).Compare(now) < 0 {
			delete(s.tokenBuckets, k)
			delete(s.lastUsed, k)
			cnt++
		}
	}
	return cnt
}
