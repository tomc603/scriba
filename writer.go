package main

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

func writer(id int, path string, outputSize int, flushSize int, keep bool, results *writerResults, wg *sync.WaitGroup) {
	var outFile *os.File
	var data []byte
	var bytesNeeded int
	var latencies []time.Duration

	readerBufSize := 32 * 1024 * 1024
	writeTotal := 0

	data = make([]byte, flushSize)
	if flushSize <= 0 {
		data = make([]byte, readerBufSize)
	}

	defer wg.Done()

	if Debug {
		log.Printf("[Writer %d] Generating random data buffer\n", id)
	}
	dr := NewDataReader(readerBufSize)
	if Debug {
		log.Printf("[Writer %d] Generated %d random bytes", id, readerBufSize)
	}

	if path == "/dev/null" {
		tmpfile, err := os.OpenFile("/dev/null", os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("[Writer %d] Error: %s\n", id, err)
			return
		}
		outFile = tmpfile
	} else {
		tmpfile, err := ioutil.TempFile(path, "output*.data")
		if err != nil {
			log.Printf("[Writer %d] Error: %s\n", id, err)
			return
		}
		outFile = tmpfile
	}
	defer outFile.Close()
	if path != "/dev/null" && !keep {
		defer os.Remove(outFile.Name())
	}

	if Debug {
		log.Printf("[Writer %d] Starting writer\n", id)
	}
	startTime := time.Now()
	for writeTotal < outputSize {
		r, err := dr.Read(data)
		if err != nil || r < flushSize {
			return
		}

		bytesNeeded = len(data)
		if outputSize - writeTotal  < len(data) {
			bytesNeeded = outputSize - writeTotal
		}

		latencyStart := time.Now()
		n, err := outFile.Write(data[:bytesNeeded])
		if err != nil {
			_ = outFile.Close()
			log.Printf("[Writer %d] Error: %s\n", id, err)
			return
		}
		if flushSize > 0 && writeTotal % flushSize == 0 {
			_ = outFile.Sync()
		}
		latencyStop := time.Now().Sub(latencyStart)
		latencies = append(latencies, latencyStop)

		writeTotal += n
	}

	if writeTotal != outputSize {
		log.Printf("ERROR: Wrote %d bytes, requested %d.\n", writeTotal, outputSize)
	}

	_ = outFile.Sync()
	_ = outFile.Close()

	results.Lock()
	results.d[path] = append(results.d[path], &throughput{id: id, bytes: writeTotal, latencies: latencies, time: time.Now().Sub(startTime)})
	results.Unlock()

	if Verbose {
		log.Printf(
			"[Writer %d] Wrote %0.2f to %s (%0.2f/s, %0.2f sec.)\n",
			id,
			float64(writeTotal) / MiB,
			outFile.Name(),
			float64(writeTotal)/MiB/time.Now().Sub(startTime).Seconds(),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}
