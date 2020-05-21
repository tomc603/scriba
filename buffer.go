package main

import (
	"math/rand"
)

type dataReader struct {
	data     []byte
	position int
}

func NewDataReader(size int) *dataReader {
	// Create 64K minimum size buffer
	if size < 65536 {
		size = 65536
	}

	data := make([]byte, size)
	dr := &dataReader{data: data, position: 0}

	_, err := rand.Read(dr.data)
	if err != nil {
		return nil
	}

	return dr
}

func (r *dataReader) Read(p []byte) (int, error) {
	// Read r.data from lastPos to either len(p) or len(r.data),
	// then cycle back around to r.data[0]

	// This is probably not the most GC friendly way to handle this operation.
	buf := make([]byte, len(p))

	copied := 0
	for copied < len(buf) {
		copied += copy(buf[copied:], r.data[r.position:])
		r.position += copied
		if r.position + 1 > len(r.data) {
			r.position = 0
		}
	}
	copy(p, buf)

	return copied, nil
}
