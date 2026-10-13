package limiter

import (
	"errors"
	"sync"
	"time"
)

var seconds float64 = float64(time.Second)
type TokenBucket struct {
	capacity   int
	refillRate float64 // assuming in per seconds
	tokens     float64
	lastAccess time.Time
	mu         sync.Mutex
	nowFunc    func() time.Time
}
type Result struct {
	Allowed    bool
	TokensLeft float64
	RetryAfter time.Duration
}

var ErrInvalidTokensNonPositive = errors.New("tokens should be greater than zero")
var ErrInvalidTokensExcessive = errors.New("tokens requested exceeds capacity")

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

func (bucket *TokenBucket) Allow(n int) (Result, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if n <= 0 {
		return Result{}, ErrInvalidTokensNonPositive
	}
	if n > bucket.capacity {
		return Result{}, ErrInvalidTokensExcessive
	}
	lastRequestWasAt := bucket.lastAccess
	bucket.lastAccess = bucket.nowFunc()
	timeSinceLastRefill := (bucket.lastAccess.Sub(lastRequestWasAt)).Seconds()
	tokens := min(bucket.tokens+(timeSinceLastRefill*bucket.refillRate), float64(bucket.capacity))
	bucket.tokens = tokens
	if float64(n) > bucket.tokens {
		retryAfter := time.Duration(((float64(n)-bucket.tokens)/bucket.refillRate )*seconds)
		return Result{false, bucket.tokens, retryAfter}, nil
	}
	bucket.tokens -= float64(n)
	return Result{true, bucket.tokens, 0}, nil
}
