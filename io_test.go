package main

import (
	"math/rand"
	"testing"
)

func BenchmarkRandomizer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < 1000000; n++ {
			_ = rand.Int63n(1000000000)
		}
	}
}

func BenchmarkZipf(b *testing.B) {
	r := rand.New(rand.NewSource(299792458))
	z := rand.NewZipf(r, 1.07, 1, 1000000000)

	for i := 0; i < b.N; i++ {
		for n := 0; n < 1000000; n++ {
			_ = z.Uint64()
		}
	}
}
