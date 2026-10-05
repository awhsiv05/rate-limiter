package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/awhsiv05/rate-limiter/internal/limiter"
)

func TestLimiter(t *testing.T) {
	bucket := limiter.NewTokenBucket(5, 1.0)
	if bucket.Allow(5) {
		fmt.Println("Allows Burst Up To Capacity : ")
		fmt.Println("ok")
	} else {
		fmt.Println("fail")
		t.Fail()
	}
	if !bucket.Allow(1) {
		fmt.Println("next request fail: ")
		fmt.Println("ok")
	} else {
		fmt.Println("fail")
		t.Fail()
	}
	time.Sleep(4 * time.Second)
	if bucket.Allow(4) {
		fmt.Println("Refilled After a Break : ")
		fmt.Println("ok")
	} else {
		fmt.Println("fail")
		t.Fail()
	}
}
