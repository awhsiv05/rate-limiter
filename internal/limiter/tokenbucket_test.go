package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type FakeClock struct {
	currentTime time.Time
	mu          sync.Mutex
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{currentTime: t, mu: sync.Mutex{}}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentTime
}
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentTime = f.currentTime.Add(d)
}

func newTestBucket(capacity int, refillRate float64, fakeClock *FakeClock) *TokenBucket {
	return newTokenBucket(capacity, refillRate, fakeClock.Now)
}

func TestAllow_BurstThenDenied(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	testBucket := newTestBucket(5, 1, fakeClock)

	Tests := []struct {
		name        string
		n           int
		expected    bool
		forwardTime time.Duration
	}{
		{"Burst up to capacity", 5, true, 0},
		{"next request Denied for 1", 1, false, 0},
	}

	for _, test := range Tests {
		t.Run(test.name, func(t *testing.T) {
			actual := testBucket.Allow(test.n)
			fakeClock.Advance(test.forwardTime)
			if actual.Allowed != test.expected {
				t.Errorf("got %t, want %t,capacity %f", actual.Allowed, test.expected, testBucket.tokens)
			}
		})
	}
}

func TestRefillAfterOneSecond(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	testBucket := newTestBucket(5, 1, fakeClock)

	Tests := []struct {
		name        string
		n           int
		expected    bool
		forwardTime time.Duration
	}{
		{"Drain and wait 1 second", 5, true, 1 * time.Second},
		{"next request Accepted for 1", 1, true, 0},
		{"next request Denied for 1", 1, false, 0},
	}

	for _, test := range Tests {
		t.Run(test.name, func(t *testing.T) {
			actual := testBucket.Allow(test.n)
			fakeClock.Advance(test.forwardTime)
			if actual.Allowed != test.expected {
				t.Errorf("got %t, want %t,capacity %f", actual.Allowed, test.expected, testBucket.tokens)
			}
		})
	}
}

func TestNeverExceedsCapacity(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	testBucket := newTestBucket(5, 1, fakeClock)

	fakeClock.Advance(100 * time.Second)
	result := testBucket.Allow(5)
	if result.BadRequest || !result.Allowed {
		t.Errorf("Allowed expected : %v | got : %v \n BadRequest expected : %v | got : %v \n ", true, result.Allowed, false, result.BadRequest)
	}

	result = testBucket.Allow(1)
	if result.BadRequest || result.Allowed {
		t.Errorf("Allowed expected : %v | got : %v \n BadRequest expected : %v | got : %v \n ", false, result.Allowed, false, result.BadRequest)
	}
}
func TestZeroAndNegative(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	testBucket := newTestBucket(5, 1, fakeClock)

	Tests := []struct {
		name        string
		n           int
		badRequest  bool
		expected    bool
		forwardTime time.Duration
	}{
		{"0 allow", 0, true, false, 0},
		{"-ve allow", -1, true, false, 0},
		{"n > capacity ", 1000, true, false, 0},
	}

	for _, test := range Tests {
		t.Run(test.name, func(t *testing.T) {
			actual := testBucket.Allow(test.n)
			fakeClock.Advance(test.forwardTime)
			if actual.BadRequest != test.badRequest || actual.Allowed != test.expected {
				t.Errorf("Allowed expected : %v | got : %v \n BadRequest expected : %v | got : %v \n ", test.expected, actual.Allowed, test.badRequest, actual.BadRequest)
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	initialTime := fakeClock.Now()
	testBucket := newTestBucket(5, 2, fakeClock)

	Tests := []struct {
		name        string
		n           int
		badRequest  bool
		expected    bool
		retryAfter  time.Time
		forwardTime time.Duration
	}{
		{"Burst up to capacity ", 5, false, true, initialTime, 0},
		{"RetryAfter for 1 token", 1, false, false, initialTime.Add(500 * time.Millisecond), 0},
		{"RetryAfter for 2 token", 2, false, false, initialTime.Add(1 * time.Second), 0},
		{"RetryAfter for 3 token", 3, false, false, initialTime.Add(1500 * time.Millisecond), 500 * time.Millisecond},
		{"RetryAfter for 1 token after wait", 1, false, true, initialTime.Add(500 * time.Millisecond), 0},
	}

	for _, test := range Tests {
		t.Run(test.name, func(t *testing.T) {
			actual := testBucket.Allow(test.n)
			fakeClock.Advance(test.forwardTime)
			if actual.BadRequest != test.badRequest || actual.Allowed != test.expected || actual.WaitTill != test.retryAfter {
				t.Errorf("Allowed expected : %v | got : %v \n BadRequest expected : %v | got : %v \n RetryAfter expected : %v | got : %v \n", test.expected, actual.Allowed, test.badRequest, actual.BadRequest, test.retryAfter, actual.WaitTill)
			}
		})
	}
}

// unreliable test use -race condition.
func TestRaceCondition(t *testing.T) {
	testBucket := NewTokenBucket(1000, 0)
	var count atomic.Int32
	var wg sync.WaitGroup

	wg.Add(2000)
	for range 2000 {
		go func() {
			defer wg.Done()
			res := testBucket.Allow(1)
			if res.Allowed {
				count.Add(1)
			}
		}()
	}

	wg.Wait()
	if count.Load() != 1000 {
		t.Errorf("got %d, want %d", count.Load(), 1000)
	}
}
