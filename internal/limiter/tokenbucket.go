package limiter

import (
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

type TokenBucket struct {
	capacity   int
	refillRate float64 // assuming in seconds
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
	nowFunc    func() time.Time
}

func nowFunc(clock *clock.Mock) time.Time {
	if clock == nil {
		return time.Now()
	}
	return clock.Now()
}
func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	Bucket := &TokenBucket{capacity: capacity,
		refillRate: refillRate,
		tokens:     float64(capacity),
		lastRefill: time.Now(),
		mu:         sync.Mutex{},
		nowFunc:    time.Now}
	return Bucket
}

func (bucket *TokenBucket) Allow(n int) bool {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	timeSinceLastRefill := time.Since(bucket.lastRefill).Seconds()
	tokens := min(bucket.tokens+(timeSinceLastRefill*bucket.refillRate), float64(bucket.capacity))
	bucket.lastRefill = bucket.nowFunc()
	bucket.tokens = tokens
	if float64(n) > bucket.tokens {
		return false
	}
	bucket.tokens -= float64(n)
	return true
}
