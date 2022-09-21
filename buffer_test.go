package main

import (
	"io"
	"math/rand"
	"testing"
	"time"
)

func TestDataReader_Read(t *testing.T) {
	smallData := make([]byte, 65536)
	largeData := make([]byte, 128*1024*1024)
	smallReader := NewDataReader(65535, PatternRand)
	equalReader := NewDataReader(65536, PatternRand)
	largeReader := NewDataReader(65537, PatternRand)
	hugeReader := NewDataReader(64*1024*1024, PatternRand)

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

func TestDataReaderValues(t *testing.T) {
	maxSize := 1024 * 1024

	// Test pattern 0x55
	data := make([]byte, maxSize)
	dataReader := NewDataReader(maxSize, Pattern55)
	t.Log("Testing value 0x55 reader.")
	if copied, _ := dataReader.Read(data); copied < len(data) {
		t.Errorf("Copy from pattern buffer failed. Coped %d bytes.\n", copied)
	}
	for i := range data {
		if data[i] != 0x55 {
			t.Errorf("Pattern 0x55 expected, read %X.\n", data[i])
		}
	}

	// Test pattern 0xAA
	data = make([]byte, maxSize)
	dataReader = NewDataReader(maxSize, PatternAA)
	t.Log("Testing value 0xAA reader.")
	if copied, _ := dataReader.Read(data); copied < len(data) {
		t.Errorf("Copy from pattern buffer failed. Coped %d bytes.\n", copied)
	}
	for i := range data {
		if data[i] != 0xAA {
			t.Errorf("Pattern 0xAA expected, read %X.\n", data[i])
		}
	}

	// Test pattern 0xFF
	data = make([]byte, maxSize)
	dataReader = NewDataReader(maxSize, PatternFF)
	t.Log("Testing value 0xFF reader.")
	if copied, _ := dataReader.Read(data); copied < len(data) {
		t.Errorf("Copy from pattern buffer failed. Coped %d bytes.\n", copied)
	}
	for i := range data {
		if data[i] != 0xFF {
			t.Errorf("Pattern 0xFF expected, read %X.\n", data[i])
		}
	}

	// Test pattern 0x00
	data = make([]byte, maxSize)
	dataReader = NewDataReader(maxSize, PatternZero)
	t.Log("Testing value 0x00 reader.")
	if copied, _ := dataReader.Read(data); copied < len(data) {
		t.Errorf("Copy from pattern buffer failed. Coped %d bytes.\n", copied)
	}
	for i := range data {
		if data[i] != 0x00 {
			t.Errorf("Pattern 0x00 expected, read %X.\n", data[i])
		}
	}
}

func TestDataReaderSizeRange(t *testing.T) {
	maxSize := 1024 * 1024
	dataReader := NewDataReader(maxSize/2, PatternRand)

	t.Log("Testing buffer sizes")
	for i := 1; i < maxSize+1; i += 1024 {
		//t.Logf("Testing buffer size %d bytes\n", i)
		data := make([]byte, i)
		d, _ := dataReader.Read(data)
		if d != i {
			t.Errorf("Reader didn't read %d bytes instead of %d.\n", d, i)
		}
	}
}

func TestDataReaderIndexes(t *testing.T) {
	maxSize := 1024 * 1024
	dataReader := NewDataReader(maxSize, PatternRand)
	buffer := make([]byte, 1)

	t.Log("Testing reader indexes")
	for i := 1; i < maxSize+1; i++ {
		d, _ := dataReader.Read(buffer)
		if d != 1 {
			t.Errorf("Reader read %d bytes instead of 1.\n", d)
		}
	}
}

func TestNewDataReader(t *testing.T) {
	t.Log("Verifying minimum read buffer size")
	if testReader := NewDataReader(256, PatternRand); len(testReader.data) < 65536 {
		t.Errorf("Reader internal buffer size is less than the minimum required. %d bytes\n", len(testReader.data))
	}

	t.Log("Verifying read buffer size matches the request")
	if testReader := NewDataReader(1024*1024, PatternRand); len(testReader.data) < 1024*1024 {
		t.Errorf("Reader internal buffer is not the size requested. %d bytes\n", len(testReader.data))
	}
}

func BenchmarkDataReader_Read(b *testing.B) {
	b.ReportAllocs()
	data := make([]byte, 64*1024*1024)
	r := NewDataReader(32*1024*1024, PatternRand)

	totalCopied := 0
	startTime := time.Now()
	for i := 0; i < b.N; i++ {
		copied, _ := r.Read(data)
		totalCopied += copied
	}
	b.Logf(
		"Copied %d, %0.2f MiB/sec",
		totalCopied,
		float64(totalCopied)/MiB/time.Now().Sub(startTime).Seconds(),
	)
}

func BenchmarkShuffle(b *testing.B) {
	b.ReportAllocs()
	var shuffles int
	r := NewDataReader(32*1024*1024, PatternRand)

	startTime := time.Now()
	for i := 0; i < b.N; i++ {
		rand.Shuffle(len(r.data), func(i, j int) {
			r.data[i], r.data[j] = r.data[j], r.data[i]
		})
		shuffles += 1
	}
	b.Logf(
		"Shuffled %d bytes %d times, %0.2f sec",
		len(r.data), shuffles, time.Now().Sub(startTime).Seconds(),
	)
}

func BenchmarkThroughput(b *testing.B) {
	b.ReportAllocs()
	readerBufSize := 32 * 1024 * 1024
	flushSize := 1 * 1024 * 1024
	outputSize := 1 * 1024 * 1024 * 1024
	data := make([]byte, flushSize)

	dr := NewDataReader(readerBufSize, PatternRand)
	tempFile := io.Discard

	for run := 0; run < b.N; run++ {
		writeTotal := 0
		startTime := time.Now()
		for writeTotal+len(data) <= outputSize {
			r, err := dr.Read(data)
			if err != nil || r < flushSize {
				b.Errorf("Reader failed. Err: %s, read: %d, requested %d\n", err, r, flushSize)
				return
			}
			n, _ := tempFile.Write(data)
			writeTotal += n
		}
		if writeTotal < outputSize {
			r, err := dr.Read(data)
			if err != nil || r < flushSize {
				b.Errorf("Reader failed. Err: %s, read: %d, requested %d\n", err, r, flushSize)
				return
			}
			n, _ := tempFile.Write(data[:outputSize-writeTotal])
			writeTotal += n
		}

		b.Logf(
			"Wrote %0.2f (%0.2f MiB/sec, %0.2f sec.)\n",
			float64(writeTotal)/MiB,
			float64(writeTotal)/MiB/time.Now().Sub(startTime).Seconds(),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}
