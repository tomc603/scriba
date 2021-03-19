package main

import (
	"math/rand"
	"time"
)

type dataReader struct {
	data     []byte
	position int
}

func NewDataReader(size int, zero bool) *dataReader {
	// Create 64K minimum size buffer
	if size < 65536 {
		size = 65536
	}

	data := make([]byte, size)
	initialPosition := rand.Intn(len(data))
	dr := &dataReader{data: data, position: initialPosition}

	if zero {
		for i, _ := range data {
			data[i] = 0
		}
	} else {
		rand.Seed(time.Now().UnixNano())
		_, err := rand.Read(dr.data)
		if err != nil {
			return nil
		}
	}

	return dr
}

func (r *dataReader) Read(p []byte) (int, error) {
	// Read r.data from lastPos to either len(p) or len(r.data),
	// then cycle back around to r.data[0]

	copied := 0
	for copied < len(p) {
		copied += copy(p[copied:], r.data[r.position:])
		r.position += copied
		if r.position >= len(r.data) {
			r.position = 0
		}
	}
	return copied, nil
}
