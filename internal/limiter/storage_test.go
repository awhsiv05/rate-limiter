package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStorageRaceCondition(t *testing.T) {
	store, err := newStore(100, 0, time.Now, 100)
	if err != nil {
		t.Fatal(err)
	}
	var count atomic.Int32
	var wg sync.WaitGroup
	wg.Add(200)
	key := "1"
	for range 200 {
		go func() {
			defer wg.Done()
			res := store.Allow(key, 1)
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

// doesn't matter
func TestIndependenceCondition(t *testing.T) {
	store, err := newStore(100, 0, time.Now, 100)
	if err != nil {
		t.Fatal(err)
	}
	// fix the Conditions later
	key1 := "1"
	key2 := "2"
	res1 := store.Allow(key1, 100)
	res2 := store.Allow(key1, 100)
	res3 := store.Allow(key2, 100)
	res4 := store.Allow(key2, 100)

	if !(res1.Allowed && !res2.Allowed && res3.Allowed && !res4.Allowed) {
		t.Errorf("key2 denied after key1 is drained ")
	}
}

func TestEvictor(t *testing.T) {

	fakeClock := NewFakeClock(time.Unix(0, 0))
	store, err := newStore(100, 1, fakeClock.Now, 100*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	key1 := "A"
	key2 := "B"
	res1 := store.Allow(key1, 100)
	if !res1.Allowed {
		t.Errorf("key1 denied")
	}
	fakeClock.Advance(99 * time.Second)
	cnt := store.evictorIdle(fakeClock.Now())
	if cnt != 0 {
		t.Errorf("got %d, want 0", cnt)
	}
	res2 := store.Allow(key2, 100)
	if !res2.Allowed {
		t.Errorf("key2 denied")
	}
	fakeClock.Advance(2 * time.Second)
	cnt = store.evictorIdle(fakeClock.Now())
	if cnt != 1 {
		t.Errorf("got %d, want 1", cnt)
	}
	key3 := "C"
	res3 := store.Allow(key3, 100)
	if !res3.Allowed {
		t.Errorf("key3 denied")
	}
	fakeClock.Advance(100 * time.Second)
	cnt = store.evictorIdle(fakeClock.Now())
	_, key2Exists := store.tokenBuckets[key2]
	if key2Exists {
		t.Errorf("Wrong Key Deleted : %s", key3)
	}
	if cnt != 1 {
		t.Errorf("got %d, want 1", cnt)
	}
}

func TestStore_StartCleanup(t *testing.T) {
	store, err := NewStore(100, 1, 100*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	key1 := "1"
	res1 := store.Allow(key1, 100)
	if !res1.Allowed {
		t.Errorf("key1 denied")
	}
	stop , err:= store.StartCleanup(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	stop()
	stop()
}
