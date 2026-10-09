package limiter

import (
	"sync"
	"time"
)

type TokenBucket struct {
	capacity   int
	refillRate float64 // assuming in per seconds
	tokens     float64
	lastAccess time.Time
	mu         sync.Mutex
	nowFunc    func() time.Time
}
type Result struct {
	BadRequest bool
	Allowed    bool
	WaitTill   time.Time
}

func newTokenBucket(capacity int, refillRate float64, nowFunc func() time.Time) *TokenBucket {
	if nowFunc == nil {
		nowFunc = time.Now
	}
	var Bucket = &TokenBucket{capacity: capacity,
		refillRate: refillRate,
		tokens:     float64(capacity),
		lastAccess: nowFunc(),
		mu:         sync.Mutex{},
		nowFunc:    nowFunc,
	}
	return Bucket
}
func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	return newTokenBucket(capacity, refillRate, nil)
}

func (bucket *TokenBucket) Allow(n int) Result {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if n <= 0 || bucket.capacity < n {
		return Result{true, false, time.Unix(1<<63-1, 0)}
	}
	lastRequestWasAt := bucket.lastAccess
	bucket.lastAccess = bucket.nowFunc()
	timeSinceLastRefill := (bucket.lastAccess.Sub(lastRequestWasAt)).Seconds()
	tokens := min(bucket.tokens+(timeSinceLastRefill*bucket.refillRate), float64(bucket.capacity))
	bucket.tokens = tokens
	if float64(n) > bucket.tokens {
		if bucket.refillRate == 0 {
			return Result{false, false, time.Unix(1<<63-1, 0)}
		}
		retryAfter := ((float64(n) - bucket.tokens) / bucket.refillRate) * 1000000000
		waitTill := bucket.lastAccess.Add(time.Duration(retryAfter))
		return Result{false, false, waitTill}
	}
	bucket.tokens -= float64(n)
	return Result{false, true, bucket.lastAccess}
}
