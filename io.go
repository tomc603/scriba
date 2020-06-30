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

const (
	FALLOC_FL_ZERO_RANGE = 0x10
)

type ReaderConfig struct {
	BlockSize   int64
	ID          int
	Results     *writerResults
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
	Results     *writerResults
	StartOffset int64
	WriteLimit  int64
	WriteTime   time.Duration
	WriterPath  string
	WriterType  uint8
}

func reader(config *ReaderConfig, wg *sync.WaitGroup) {
	var (
		bytesToRead int64
		fileSize    int64
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
	for {
		if config.ReadLimit > 0 && readTotal >= config.ReadLimit {
			// A data limit has been specified and we've reached or exceeded it.
			break
		}
		if config.ReadTime > 0 && time.Now().Sub(startTime) >= config.ReadTime {
			// A time limit has been specified and we've reached or exceeded it.
			break
		}
		bytesToRead = config.BlockSize
		if config.ReadLimit > 0 && config.ReadLimit-readTotal < bytesToRead {
			bytesToRead = config.ReadLimit - readTotal
		}

		latencyStart := time.Now()
		switch config.ReaderType {
		case Random:
			randPos := rand.Int63n(fileSize - bytesToRead)
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

	if readTotal != config.ReadLimit {
		log.Printf("ERROR: Read %d bytes, requested %d.\n", readTotal, config.ReadLimit)
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
		lastPos     int64
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
		lastPos = off
	}

	if Debug {
		log.Printf("[Writer %d] Starting writer\n", config.ID)
	}
	startTime := time.Now()
	for {
		if config.WriteLimit > 0 && writeTotal >= config.WriteLimit {
			// A size limit has been specified, and we've reached or exceeded that limit.
			break
		}
		if config.WriteTime != 0 && time.Now().Sub(startTime) >= config.WriteTime {
			// A time limit has been specified, and we've reached or exceeded that limit.
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

		latencyStart := time.Now()
		switch config.WriterType {
		case Random:
			randPos := rand.Int63n(config.FileSize - config.BlockSize)
			if _, err := workFile.Seek(randPos, 0); err != nil {
				log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, randPos, err)
			}
		case Repeat:
			if _, err := workFile.Seek(config.StartOffset, 0); err != nil {
				log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, config.StartOffset, err)
			}
		default:
			if lastPos+bytesNeeded > config.FileSize {
				if Debug {
					log.Printf("[Writer %d] %s EOF, seeking to 0", config.ID, config.WriterPath)
				}

				if seekPos, err := workFile.Seek(0, 0); err != nil {
					log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, 0, err)
				} else {
					lastPos = seekPos
				}
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
		lastPos += int64(n)
	}

	if config.WriteLimit > 0 && writeTotal != config.WriteLimit {
		log.Printf("ERROR: Wrote %d bytes, requested %d.\n", writeTotal, config.WriteLimit)
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
