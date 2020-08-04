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
	FileSize    int64
	ID          int
	Results     *IOStats
	ReadLimit   int64
	ReadTime    time.Duration
	ReaderPath  string
	ReaderType  uint8
	StartOffset int64
}

type WriterConfig struct {
	BatchSize   int64
	BlockSize   int64
	FileSize    int64
	ID          int
	Results     *IOStats
	StartOffset int64
	WriteLimit  int64
	WriteTime   time.Duration
	WriterPath  string
	WriterType  uint8
}

func reader(config *ReaderConfig, wg *sync.WaitGroup) {
	var (
		bytesToRead  int64
		latencies    []time.Duration
		readTotal    int64
		seek         bool
		seekPosition int64
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
	for {
		seek = false

		if Stop {
			// The user has interrupted us, so stop reading and return normally.
			break
		}
		if config.ReadLimit > 0 && readTotal >= config.ReadLimit {
			// A data limit has been specified and we've reached or exceeded it.
			log.Printf("[Reader %d]: Data limit has elapsed. Stopping reader routine.\n", config.ID)
			break
		}
		if config.ReadTime > 0 && time.Now().Sub(startTime) >= config.ReadTime {
			// A time limit has been specified and we've reached or exceeded it.
			log.Printf("[Reader %d]: Time limit has elapsed. Stopping reader routine.\n", config.ID)
			break
		}
		bytesToRead = config.BlockSize
		if config.ReadLimit > 0 && config.ReadLimit-readTotal < bytesToRead {
			bytesToRead = config.ReadLimit - readTotal
		}

		// If we aren't performing sequential I/O, calculate the position to seek for the next operation
		switch config.ReaderType {
		case Random:
			seekPosition = rand.Int63n(config.FileSize - bytesToRead)
			seek = true
		case Repeat:
			seekPosition = config.StartOffset
			seek = true
		}

		// Don't count the position calculation in the latency math
		latencyStart := time.Now()
		if seek {
			if _, err := workFile.Seek(seekPosition, 0); err != nil {
				log.Printf("[Reader %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.ReaderPath, seekPosition, err)
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
		if config.Results != nil {
			latencies = append(latencies, latencyStop)
		}
	}

	if config.Results != nil {
		config.Results.Lock()
		config.Results.ReadThroughput[config.ReaderPath] = append(config.Results.ReadThroughput[config.ReaderPath], &Throughput{ID: config.ID, Bytes: readTotal, Latencies: latencies, Time: time.Now().Sub(startTime)})
		config.Results.Unlock()
	}

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
		bytesNeeded  int64
		data         []byte
		lastPos      int64
		latencies    []time.Duration
		seek         bool
		seekPosition int64
		writeTotal   int64
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
		lastPos = off
	}

	if Debug {
		log.Printf("[Writer %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for {
		seek = false

		if Stop {
			// The user has interrupted us, so stop writing and return normally.
			break
		}
		if config.WriteLimit > 0 && writeTotal >= config.WriteLimit {
			// A size limit has been specified, and we've reached or exceeded that limit.
			log.Printf("[Writer %d]: Data limit has elapsed. Stopping writer routine.\n", config.ID)
			break
		}
		if config.WriteTime != 0 && time.Now().Sub(startTime) >= config.WriteTime {
			// A time limit has been specified, and we've reached or exceeded that limit.
			log.Printf("[Writer %d]: Time limit has elapsed. Stopping writer routine.\n", config.ID)
			break
		}
		r, err := dr.Read(data)
		if err != nil || int64(r) < config.BlockSize {
			return
		}

		bytesNeeded = int64(len(data))
		if config.WriteLimit > 0 && config.WriteLimit-writeTotal < int64(len(data)) {
			bytesNeeded = config.WriteLimit - writeTotal
		}

		switch config.WriterType {
		case Random:
			seekPosition = rand.Int63n(config.FileSize - config.BlockSize)
			seek = true
		case Repeat:
			seekPosition = config.StartOffset
			seek = true
		default:
			if lastPos+bytesNeeded > config.FileSize {
				if Debug {
					log.Printf("[Writer %d] %s EOF, seeking to 0", config.ID, config.WriterPath)
				}
				seekPosition = 0
				seek = true
			}
		}

		// Don't count the position calculation in the latency math
		latencyStart := time.Now()
		if seek {
			if seekPos, err := workFile.Seek(seekPosition, 0); err != nil {
				log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, seekPosition, err)
			} else {
				lastPos = seekPos
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
		if config.Results != nil {
			latencies = append(latencies, latencyStop)
		}
		writeTotal += int64(n)
		lastPos += int64(n)
	}

	_ = workFile.Sync()
	_ = workFile.Close()

	if config.Results != nil {
		config.Results.Lock()
		config.Results.WriteThroughput[config.WriterPath] = append(config.Results.WriteThroughput[config.WriterPath], &Throughput{ID: config.ID, Bytes: writeTotal, Latencies: latencies, Time: time.Now().Sub(startTime)})
		config.Results.Unlock()
	}

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
