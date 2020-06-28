package main

import (
	"io"
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

type ReaderConfig struct {
	BlockSize   int64
	ID          int
	TotalSize   int64
	Results     *writerResults
	ReaderPath  string
	ReaderType  uint8
	StartOffset int64
}

type WriterConfig struct {
	FlushSize   int64
	ID          int
	Keep        bool
	OutputSize  int64
	Results     *writerResults
	StartOffset int64
	WriterPath  string
	WriterType  uint8
}

func reader(config *ReaderConfig, wg *sync.WaitGroup) {
	var (
		workFile  *os.File
		latencies []time.Duration
		readTotal int64 = 0
	)

	buf := make([]byte, config.BlockSize)

	defer wg.Done()

	workFile, err := os.OpenFile(config.ReaderPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("[Reader %d] Error: %s\n", config.ID, err)
		return
	}
	defer workFile.Close()
	workFile.Seek(config.StartOffset, 0)

	if Debug {
		log.Printf("[Reader %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for readTotal < config.TotalSize {
		latencyStart := time.Now()
		n, err := workFile.Read(buf)
		latencyStop := time.Now().Sub(latencyStart)
		readTotal += int64(n)
		if err != nil {
			if err == io.EOF {
				// We might read a partial buffer here, but detecting EOF and
				// seeking to the beginning is the easiest way to continue reading
				// while we need more data. This should be extremely rare.
				if Debug {
					log.Printf("[Reader %d]: Reached EOF, seeking to 0\n", config.ID)
				}
				workFile.Seek(0, 0)
				continue
			}
			log.Printf("[Reader %d] Error: %s\n", config.ID, err)
			return
		}
		// Only include latencies from full transactions so we don't skew data with
		// partial buffer fills
		latencies = append(latencies, latencyStop)
	}

	if readTotal != config.TotalSize {
		log.Printf("ERROR: Read %d bytes, requested %d.\n", readTotal, config.TotalSize)
	}

	config.Results.Lock()
	config.Results.d[config.ReaderPath] = append(config.Results.d[config.ReaderPath], &throughput{id: config.ID, bytes: readTotal, latencies: latencies, time: time.Now().Sub(startTime)})
	config.Results.Unlock()

	if Verbose {
		log.Printf(
			"[Reader %d] Read %0.2f to %s (%0.2f/s, %0.2f sec.)\n",
			config.ID,
			float64(readTotal)/MiB,
			workFile.Name(),
			float64(readTotal)/MiB/time.Now().Sub(startTime).Seconds(),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}

func writer(config *WriterConfig, wg *sync.WaitGroup) {
	var (
		outFile     *os.File
		data        []byte
		bytesNeeded int64
		latencies   []time.Duration
		writeTotal  int64 = 0
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

	outFile, err := os.OpenFile(config.WriterPath, os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Writer %d] Error: %s\n", config.ID, err)
		return
	}
	defer outFile.Close()
	outFile.Seek(config.StartOffset, 0)

	if Debug {
		log.Printf("[Writer %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for writeTotal < config.OutputSize {
		r, err := dr.Read(data)
		if err != nil || int64(r) < config.FlushSize {
			return
		}

		bytesNeeded = int64(len(data))
		if config.OutputSize-writeTotal < int64(len(data)) {
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

		writeTotal += int64(n)
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
