package main

import (
	"log"
	"math/rand"
	"testing"
)

func BenchmarkRandomizer(b *testing.B) {
	var randpos int64

	for i := 0; i < b.N; i++ {
		randpos = rand.Int63n(1024 * TB)
	}
	log.Printf("Final random position: %d", randpos)
}
