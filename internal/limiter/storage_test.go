package limiter

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreBucketCreation(t *testing.T) {
	store, err := NewStore(100, 1, 150*time.Second)
	if err != nil {
		t.Error(err)
	}
	key := "A"
	_, err = store.Allow(key, 0)
	if err == nil || !errors.Is(err, ErrInvalidTokensNonPositive) {
		t.Errorf("error should be %v got : %v", ErrInvalidTokensNonPositive, err)
	}
	_, err = store.Allow(key, 1000)
	if err == nil || !errors.Is(err, ErrInvalidTokensExcessive) {
		t.Errorf("error should be %v got : %v", ErrInvalidTokensExcessive, err)
	}
}

func TestStorageRaceCondition(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	store, err := newStore(100, 1, fakeClock.Now, 100*time.Second)
	if err != nil {
		t.Error(err)
	}
	var count atomic.Int32
	var wg sync.WaitGroup
	wg.Add(200)
	key := "1"
	for range 200 {
		go func() {
			defer wg.Done()
			res, err := store.Allow(key, 1)
			if err != nil {
				t.Errorf("%v", err)
			}
			if res.Allowed {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 100 {
		t.Errorf("got %d, want 100", count.Load())
	}
}

func TestIndependenceCondition(t *testing.T) {
	fakeClock := NewFakeClock(time.Unix(0, 0))
	store, err := newStore(100, 1, fakeClock.Now, 100*time.Second)
	if err != nil {
		t.Error(err)
	}
	key1 := "1"
	key2 := "2"
	res1, err := store.Allow(key1, 100)
	if err != nil {
		t.Error(err)
	}
	res2, err := store.Allow(key1, 100)
	if err != nil {
		t.Error(err)
	}
	res3, err := store.Allow(key2, 100)
	if err != nil {
		t.Error(err)
	}
	res4, err := store.Allow(key2, 100)
	if err != nil {
		t.Error(err)
	}
	if !(res1.Allowed) {
		t.Errorf("key1 should be allowed as Not drained yet")
	}
	if res1.Allowed && res2.Allowed {
		t.Errorf("bucket with Key1 not drained")
	}
	if res3.Allowed && res4.Allowed {
		t.Errorf("bucket with Key2 not drained")
	}
	if res1.Allowed && !res2.Allowed && !res3.Allowed {
		t.Errorf("bucket with Key1 drained and bucket with Key2 Denied without draining")
	}
}

func TestEvictor(t *testing.T) {

	fakeClock := NewFakeClock(time.Unix(0, 0))
	store, err := newStore(100, 1, fakeClock.Now, 100*time.Second)
	if err != nil {
		t.Error(err)
	}
	key1 := "A"
	key2 := "B"
	res1, err := store.Allow(key1, 100)
	if err != nil {
		t.Error(err)
	}
	if !res1.Allowed {
		t.Errorf("key1 denied")
	}
	fakeClock.Advance(99 * time.Second)
	cnt := store.evictorIdle(fakeClock.Now())
	if cnt != 0 {
		t.Errorf("got %d, want 0", cnt)
	}
	res2, err := store.Allow(key2, 100)
	if err != nil {
		t.Error(err)
	}
	if !res2.Allowed {
		t.Errorf("key2 denied")
	}
	fakeClock.Advance(2 * time.Second)
	cnt = store.evictorIdle(fakeClock.Now())
	if cnt != 1 {
		t.Errorf("got %d, want 1", cnt)
	}
	key3 := "C"
	res3, err := store.Allow(key3, 100)
	if err != nil {
		t.Error(err)
	}
	if !res3.Allowed {
		t.Errorf("key3 denied")
	}
	fakeClock.Advance(100 * time.Second)
	cnt = store.evictorIdle(fakeClock.Now())
	_, key2Exists := store.tokenBuckets[key2]
	if key2Exists {
		t.Errorf("Wrong Key Deleted : %s", key2)
	}
	if _, key3Exists := store.tokenBuckets[key3]; !key3Exists {
		t.Errorf("Wrong Key Deleted : %s", key3)
	}
	if cnt != 1 {
		t.Errorf("got %d, want 1", cnt)
	}
}

func TestStore_StopCleanup(t *testing.T) {
	store, err := NewStore(100, 1, 100*time.Second)
	if err != nil {
		t.Error(err)
	}
	key1 := "1"
	res1, err := store.Allow(key1, 100)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !res1.Allowed {
		t.Errorf("key1 denied")
	}
	stop, err := store.StartCleanup(10 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}
	stop()
	stop()
}

func TestStore_StartCleanup(t *testing.T) {
	store, err := NewStore(100, 1, 100*time.Second)
	if err != nil {
		t.Error(err)
	}
	_, err = store.StartCleanup(0 * time.Millisecond)
	if errors.Is(err, ErrCapacityNonPositive) {
		t.Errorf("error should be %v got : %v", ErrCapacityNonPositive, err)
	}
}
