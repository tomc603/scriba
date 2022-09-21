package main

import (
	"math/rand"
	"time"
)

const (
	PatternZero = iota
	Pattern55
	PatternAA
	PatternFF
	PatternRand
)

type dataReader struct {
	data     []byte
	position int
}

func NewDataReader(size int, pattern int) *dataReader {
	//var total int
	// Create 64K minimum size buffer
	if size < 65536 {
		size = 65536
	}

	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	data := make([]byte, size)
	initialPosition := rand.Intn(len(data))

	// TODO: Handle pattern rather than zero
	switch pattern {
	case Pattern55:
		for i := range data {
			data[i] = 0x55
		}
	case PatternAA:
		for i := range data {
			data[i] = 0xAA
		}
	case PatternFF:
		for i := range data {
			data[i] = 0xFF
		}
	case PatternZero:
		for i := range data {
			data[i] = 0x00
		}
	case PatternRand:
		r.Read(data)
	}

	dr := &dataReader{data: data, position: initialPosition}
	return dr
}

func (r *dataReader) Read(p []byte) (int, error) {
	// Read r.data from lastPos to either len(p) or len(r.data),
	// then cycle back around to r.data[0]

	total := 0
	for total < len(p) {
		copied := copy(p[total:], r.data[r.position:])
		total += copied
		r.position += copied
		if r.position >= len(r.data) {
			r.position = 0
		}
	}
	return total, nil
}
