package limiter

import (
	"sync"
	"testing"
	"time"
)

type FakeClock struct {
	currentTime time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{currentTime: t}
}

func (f *FakeClock) Now() time.Time {
	return f.currentTime
}
func (f *FakeClock) Advance(d time.Duration) {
	f.currentTime = f.currentTime.Add(d)
}

func newTestBucket(capacity int, refillRate float64, fakeClock *FakeClock) *TokenBucket {
	return &TokenBucket{capacity: capacity, refillRate: refillRate, tokens: float64(capacity), lastRefill: fakeClock.Now(), mu: sync.Mutex{}, nowFunc: fakeClock.Now}
}
func TestLimiter(t *testing.T) {
	Tests := []struct {
		name        string
		n           int
		expected    bool
		forwardTime time.Duration
	}{
		{"Negative requests", -1, false, 250 * time.Millisecond},
		{"Burst up to capacity", 5, true, 500 * time.Millisecond},
		{"next request Denied for 1", 1, false, 1 * time.Second},
		{"next request Denied for 2", 2, false, 1 * time.Second},
		{"next request Denied for 3", 3, false, 1 * time.Second},
		{"next request Denied for 4", 4, false, 1 * time.Second},
		{"next request Denied for 5", 5, false, 2 * time.Second},
		{"refilled after all the requests to 5", 5, true, 500 * time.Millisecond},
	}
	fakeClock := NewFakeClock(time.Now())
	testBucket := newTestBucket(5, 1, fakeClock)

	for _, test := range Tests {
		t.Run(test.name, func(t *testing.T) {
			actual := testBucket.Allow(test.n)
			fakeClock.Advance(test.forwardTime)
			if actual != test.expected {
				t.Fatalf("got %t, want %t,capacity %f", actual, test.expected, testBucket.tokens)
			}
		})
	}
}
