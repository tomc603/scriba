package main

import (
	"testing"
	"time"
)

func TestDataReader_Read(t *testing.T) {
	smallData := make([]byte, 65536)
	largeData := make([]byte, 128 * 1024 * 1024)
	smallReader := NewDataReader(65535)
	equalReader := NewDataReader(65536)
	largeReader := NewDataReader(65537)
	hugeReader := NewDataReader(64 * 1024 * 1024)

	// Test copying data into a small byte slice
	t.Log("Testing small reader buffer, small target")
	if copied, _ := smallReader.Read(smallData); copied < len(smallData) {
		t.Errorf("Copy from smaller buffer failed. Coped %d bytes.\n", copied)
	}
	t.Log("Testing equal sized small reader buffer and target")
	if copied, _ := equalReader.Read(smallData); copied < len(smallData) {
		t.Errorf("Copy from equally sized buffer failed. Coped %d bytes.\n", copied)
	}
	t.Log("Testing large reader buffer, small target")
	if copied, _ := largeReader.Read(smallData); copied < len(smallData) {
		t.Errorf("Copy from larger buffer failed. Coped %d bytes.\n", copied)
	}
	t.Log("Testing huge reader buffer, small target")
	if copied, _ := hugeReader.Read(smallData); copied < len(smallData) {
		t.Errorf("Copy from huge buffer failed. Coped %d bytes.\n", copied)
	}

	// Test copying data into a huge byte slice
	t.Log("Testing small reader buffer, large target")
	if copied, _ := smallReader.Read(largeData); copied < len(largeData) {
		t.Errorf("Copy from smaller buffer failed. Coped %d bytes.\n", copied)
	}
	t.Log("Testing equal sized large reader buffer and target")
	if copied, _ := equalReader.Read(largeData); copied < len(largeData) {
		t.Errorf("Copy from equally sized buffer failed. Coped %d bytes.\n", copied)
	}
	t.Log("Testing large reader buffer, large target")
	if copied, _ := largeReader.Read(largeData); copied < len(largeData) {
		t.Errorf("Copy from larger buffer failed. Coped %d bytes.\n", copied)
	}
	t.Log("Testing huge reader buffer, large target")
	if copied, _ := hugeReader.Read(largeData); copied < len(largeData) {
		t.Errorf("Copy from huge buffer failed. Coped %d bytes.\n", copied)
	}
}

func TestNewDataReader(t *testing.T) {
	t.Log("Verifying minimum read buffer size")
	if testReader := NewDataReader(256); len(testReader.data) < 65536 {
		t.Errorf("Reader internal buffer size is less than the minimum required. %d bytes\n", len(testReader.data))
	}

	t.Log("Verifying read buffer size matches the request")
	if testReader := NewDataReader( 1024 * 1024); len(testReader.data) < 1024 * 1024 {
		t.Errorf("Reader internal buffer is not the size requested. %d bytes\n", len(testReader.data))
	}
}

func BenchmarkDataReader_Read(b *testing.B) {
	data := make([]byte, 65536)
	r := NewDataReader(32 * 1024 * 1024)

	totalCopied := 0
	startTime := time.Now()
	for i := 0; i < b.N; i++ {
		copied, _ := r.Read(data)
		totalCopied += copied
	}
	b.Logf("Copied %d, %0.2f MB/sec", totalCopied, float64(totalCopied / 1024 / 1024) / time.Now().Sub(startTime).Seconds())
}
