package main

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

const (
	Sequential uint8 = iota
	Random
	Repeat
)

type WriterConfig struct {
	FlushSize  uint64
	ID         int
	Keep       bool
	OutputSize uint64
	Results    *writerResults
	WriterPath string
	WriterType uint8
}

func writer(config *WriterConfig, wg *sync.WaitGroup) {
	var (
		outFile     *os.File
		data        []byte
		bytesNeeded uint64
		latencies   []time.Duration
		writeTotal  uint64 = 0
	)

	readerBufSize := 32 * 1024 * 1024

	data = make([]byte, config.FlushSize)
	if config.FlushSize <= 0 {
		data = make([]byte, readerBufSize)
	}

	defer wg.Done()

	if Debug {
		log.Printf("[Writer %d] Generating random data buffer\n", config.ID)
	}
	dr := NewDataReader(readerBufSize)
	if Debug {
		log.Printf("[Writer %d] Generated %d random bytes", config.ID, readerBufSize)
	}

	if config.WriterPath == "/dev/null" {
		tmpfile, err := os.OpenFile("/dev/null", os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("[Writer %d] Error: %s\n", config.ID, err)
			return
		}
		outFile = tmpfile
	} else {
		tmpfile, err := ioutil.TempFile(config.WriterPath, "output*.data")
		if err != nil {
			log.Printf("[Writer %d] Error: %s\n", config.ID, err)
			return
		}
		outFile = tmpfile
		if err := outFile.Truncate(int64(config.OutputSize)); err != nil {
			log.Printf("[Writer %d] Error: Unable to truncate file. %s\n", config.ID, err)
			return
		}
		outFile.Sync()
	}
	defer outFile.Close()
	if config.WriterPath != "/dev/null" && !config.Keep {
		defer os.Remove(outFile.Name())
	}

	if Debug {
		log.Printf("[Writer %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for writeTotal < config.OutputSize {
		r, err := dr.Read(data)
		if err != nil || uint64(r) < config.FlushSize {
			return
		}

		bytesNeeded = uint64(len(data))
		if config.OutputSize-writeTotal < uint64(len(data)) {
			bytesNeeded = config.OutputSize - writeTotal
		}

		latencyStart := time.Now()
		n, err := outFile.Write(data[:bytesNeeded])
		if err != nil {
			_ = outFile.Close()
			log.Printf("[Writer %d] Error: %s\n", config.ID, err)
			return
		}
		if config.FlushSize > 0 && writeTotal%config.FlushSize == 0 {
			_ = outFile.Sync()
		}
		latencyStop := time.Now().Sub(latencyStart)
		latencies = append(latencies, latencyStop)

		writeTotal += uint64(n)
	}

	if writeTotal != config.OutputSize {
		log.Printf("ERROR: Wrote %d bytes, requested %d.\n", writeTotal, config.OutputSize)
	}

	_ = outFile.Sync()
	_ = outFile.Close()

	config.Results.Lock()
	config.Results.d[config.WriterPath] = append(config.Results.d[config.WriterPath], &throughput{id: config.ID, bytes: writeTotal, latencies: latencies, time: time.Now().Sub(startTime)})
	config.Results.Unlock()

	if Verbose {
		log.Printf(
			"[Writer %d] Wrote %0.2f to %s (%0.2f/s, %0.2f sec.)\n",
			config.ID,
			float64(writeTotal)/MiB,
			outFile.Name(),
			float64(writeTotal)/MiB/time.Now().Sub(startTime).Seconds(),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}
