package limiter

import (
	"sync"
	"time"
)

type TokenBucket struct {
	capacity   int
	refillRate float64 // assuming in per seconds
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
	nowFunc    func() time.Time
}

func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	nowFunc := time.Now
	var Bucket = &TokenBucket{capacity: capacity,
		refillRate: refillRate,
		tokens:     float64(capacity),
		lastRefill: nowFunc(),
		mu:         sync.Mutex{},
		nowFunc:    nowFunc,
	}
	return Bucket
}

func (bucket *TokenBucket) Allow(n int) bool {
	if n < 0 {
		return false
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	timeSinceLastRefill := (bucket.nowFunc().Sub(bucket.lastRefill)).Seconds()
	tokens := min(bucket.tokens+(timeSinceLastRefill*bucket.refillRate), float64(bucket.capacity))
	bucket.lastRefill = bucket.nowFunc()
	bucket.tokens = tokens
	if float64(n) > bucket.tokens {
		return false
	}
	bucket.tokens -= float64(n)
	return true
}
