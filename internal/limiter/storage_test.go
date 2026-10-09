package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestStorageRaceCondition(t *testing.T) {
	store := NewStore(100, 0)
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

func TestIndependenceCondition(t *testing.T) {
	store := NewStore(100, 0)
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
