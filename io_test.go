package main

import (
	"math/rand"
	"testing"
	"time"
)

func TestRandomizer(t *testing.T) {
	t1 := time.Now()
	for n := 0; n < 1000000; n++ {
		_ = rand.Int63n(1000000000)
	}
	testDuration := time.Now().Sub(t1)
	t.Logf("Random Int63n %0.2f/sec, %0.2f sec.", 1000000.0/testDuration.Seconds(), testDuration.Seconds())
}

func BenchmarkRandomizer(b *testing.B) {
	t1 := time.Now()
	for i := 0; i < b.N; i++ {
		for n := 0; n < 1000000; n++ {
			_ = rand.Int63n(1000000000)
		}
	}
	testDuration := time.Now().Sub(t1)
	b.Logf("Random Int63n %0.2f/sec, %0.2f sec.", 1000000.0/testDuration.Seconds(), testDuration.Seconds())
}

func BenchmarkZipf(b *testing.B) {
	r := rand.New(rand.NewSource(299792458))
	z := rand.NewZipf(r, 1.07, 1, 1000000000)

	t1 := time.Now()
	for i := 0; i < b.N; i++ {
		for n := 0; n < 1000000; n++ {
			_ = z.Uint64()
		}
	}
	testDuration := time.Now().Sub(t1)
	b.Logf("Zipf Uint64 %0.2f/sec, %0.2f sec.", 1000000.0/testDuration.Seconds(), testDuration.Seconds())
}
