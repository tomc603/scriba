package main

import (
	"io"
	"log"
	"math/rand"
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
	BatchSize   int64
	BlockSize   int64
	ID          int
	TotalSize   int64
	Results     *writerResults
	StartOffset int64
	WriterPath  string
	WriterType  uint8
}

func reader(config *ReaderConfig, wg *sync.WaitGroup) {
	var (
		bytesToRead int64
		latencies   []time.Duration
		readTotal   int64
	)

	buf := make([]byte, config.BlockSize)

	defer wg.Done()

	workFile, err := os.OpenFile(config.ReaderPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("[Reader %d] Error: %s\n", config.ID, err)
		return
	}
	defer workFile.Close()
	if off, err := workFile.Seek(config.StartOffset, 0); err != nil {
		log.Printf("[Reader %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.ReaderPath, config.StartOffset, err)
	} else {
		if Debug {
			log.Printf("[Reader %d] New offset %s@%d", config.ID, config.ReaderPath, off)
		}
	}

	if Debug {
		log.Printf("[Reader %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for readTotal < config.TotalSize {
		bytesToRead = config.BlockSize
		if config.TotalSize-readTotal < bytesToRead {
			bytesToRead = config.TotalSize - readTotal
		}

		latencyStart := time.Now()
		switch config.ReaderType {
		case Random:
			randPos := rand.Int63n(config.TotalSize - bytesToRead)
			if _, err := workFile.Seek(randPos, 0); err != nil {
				log.Printf("[Reader %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.ReaderPath, randPos, err)
			}
		case Repeat:
			if _, err := workFile.Seek(config.StartOffset, 0); err != nil {
				log.Printf("[Reader %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.ReaderPath, config.StartOffset, err)
			}
		}

		n, err := workFile.Read(buf[:bytesToRead])
		readTotal += int64(n)
		if err != nil {
			if err == io.EOF {
				// We might read a partial buffer here, but detecting EOF and
				// seeking to the beginning is the easiest way to continue reading
				// while we need more data. This should be extremely rare.
				if Debug {
					log.Printf("[Reader %d]: Reached EOF, seeking to 0\n", config.ID)
				}
				if _, err := workFile.Seek(0, 0); err != nil {
					log.Printf("[Reader %d]: ERROR Unable to seek to beginning of %s. %s\n", config.ID, config.ReaderPath, err)
				}
				continue
			}
			log.Printf("[Reader %d] ERROR: %s\n", config.ID, err)
			return
		}
		latencyStop := time.Now().Sub(latencyStart)
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
			"[Reader %d] Read %0.2f MiB from %s (%0.2f MiB/sec, %0.2f sec.)\n",
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
		bytesNeeded int64
		data        []byte
		latencies   []time.Duration
		writeTotal  int64
	)

	readerBufSize := 33554432
	data = make([]byte, config.BlockSize)

	defer wg.Done()

	if Debug {
		log.Printf("[Writer %d] Generating random data buffer\n", config.ID)
	}
	dr := NewDataReader(readerBufSize)
	if Debug {
		log.Printf("[Writer %d] Generated %d random bytes", config.ID, readerBufSize)
	}

	workFile, err := os.OpenFile(config.WriterPath, os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Writer %d] Error: %s\n", config.ID, err)
		return
	}
	defer workFile.Close()
	if off, err := workFile.Seek(config.StartOffset, 0); err != nil {
		log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, config.StartOffset, err)
	} else {
		if Debug {
			log.Printf("[Writer %d] New offset %s@%d", config.ID, config.WriterPath, off)
		}
	}

	if Debug {
		log.Printf("[Writer %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for writeTotal < config.TotalSize {
		r, err := dr.Read(data)
		if err != nil || int64(r) < config.BlockSize {
			return
		}

		bytesNeeded = int64(len(data))
		if config.TotalSize-writeTotal < int64(len(data)) {
			bytesNeeded = config.TotalSize - writeTotal
		}

		latencyStart := time.Now()
		switch config.WriterType {
		case Random:
			randPos := rand.Int63n(config.TotalSize - config.BlockSize)
			if _, err := workFile.Seek(randPos, 0); err != nil {
				log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, randPos, err)
			}
		case Repeat:
			if _, err := workFile.Seek(config.StartOffset, 0); err != nil {
				log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, config.StartOffset, err)
			}
		}

		n, err := workFile.Write(data[:bytesNeeded])
		if err != nil {
			_ = workFile.Close()
			log.Printf("[Writer %d] Error: %s\n", config.ID, err)
			return
		}
		if config.BatchSize > 0 && writeTotal%config.BatchSize == 0 {
			_ = workFile.Sync()
		}
		latencyStop := time.Now().Sub(latencyStart)
		latencies = append(latencies, latencyStop)
		writeTotal += int64(n)
	}

	if writeTotal != config.TotalSize {
		log.Printf("ERROR: Wrote %d bytes, requested %d.\n", writeTotal, config.TotalSize)
	}

	_ = workFile.Sync()
	_ = workFile.Close()

	config.Results.Lock()
	config.Results.d[config.WriterPath] = append(config.Results.d[config.WriterPath], &throughput{id: config.ID, bytes: writeTotal, latencies: latencies, time: time.Now().Sub(startTime)})
	config.Results.Unlock()

	if Verbose {
		log.Printf(
			"[Writer %d] Wrote %0.2f MiB to %s (%0.2f MiB/sec, %0.2f sec.)\n",
			config.ID,
			float64(writeTotal)/MiB,
			workFile.Name(),
			float64(writeTotal)/MiB/time.Now().Sub(startTime).Seconds(),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}
