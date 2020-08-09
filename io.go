package main

import (
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

const (
	Sequential uint8 = iota
	Random
	Repeat
)

type ReaderConfig struct {
	BlockSize       int64
	FileSize        int64
	ID              int
	Results         *IOStats
	ReadLimit       int64
	ReadTime        time.Duration
	ReaderPath      string
	ReaderType      uint8
	StartOffset     int64
	ThroughputBytes int64
	ThroughputTime  time.Duration
}

type WriterConfig struct {
	BatchSize       int64
	BlockSize       int64
	FileSize        int64
	ID              int
	Results         *IOStats
	StartOffset     int64
	ThroughputBytes int64
	ThroughputTime  time.Duration
	WriteLimit      int64
	WriteTime       time.Duration
	WriterPath      string
	WriterType      uint8
}

func dropPageCache() {
	//	/proc/sys/vm/drop_caches
	if runtime.GOOS != "linux" {
		if Verbose {
			log.Printf("OS is not Linux, unable to drop PageCache.")
		}
		return
	}

	procFile, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_RDWR, 0666)
	if err != nil {
		log.Printf("Error: %s\n", err)
		return
	}

	_, err = procFile.WriteString("1")
	if err != nil {
		log.Printf("Unable to write to /proc/sys/vm/drop_caches. %s", err)
	}
	_ = procFile.Sync()

	err = procFile.Close()
	if err != nil {
		log.Printf("Unable to close /proc/sys/vm/drop_caches. %s", err)
	}
}

func reader(config *ReaderConfig, wg *sync.WaitGroup) {
	var (
		bytesToRead  int64
		latencies    []time.Duration
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
		if config.ReadLimit > 0 && config.ThroughputBytes >= config.ReadLimit {
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
		if config.ReadLimit > 0 && config.ReadLimit-config.ThroughputBytes < bytesToRead {
			bytesToRead = config.ReadLimit - config.ThroughputBytes
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
		config.ThroughputBytes += int64(n)
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
	config.ThroughputTime = time.Now().Sub(startTime)

	if config.Results != nil {
		config.Results.Lock()
		config.Results.ReadThroughput[config.ReaderPath] = append(config.Results.ReadThroughput[config.ReaderPath], &Throughput{ID: config.ID, Bytes: config.ThroughputBytes, Latencies: latencies, Time: config.ThroughputTime})
		config.Results.Unlock()
	}

	if Verbose {
		log.Printf(
			"[Reader %d] Read %0.2f MiB from %s (%0.2f MiB/sec, %0.2f sec.)\n",
			config.ID,
			float64(config.ThroughputBytes)/MiB,
			workFile.Name(),
			float64(config.ThroughputBytes)/MiB/config.ThroughputTime.Seconds(),
			config.ThroughputTime.Seconds(),
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
		if config.WriteLimit > 0 && config.ThroughputBytes >= config.WriteLimit {
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
		if config.WriteLimit > 0 && config.WriteLimit-config.ThroughputBytes < int64(len(data)) {
			bytesNeeded = config.WriteLimit - config.ThroughputBytes
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
		if config.BatchSize > 0 && config.ThroughputBytes%config.BatchSize == 0 {
			_ = workFile.Sync()
		}
		latencyStop := time.Now().Sub(latencyStart)
		if config.Results != nil {
			latencies = append(latencies, latencyStop)
		}
		config.ThroughputBytes += int64(n)
		lastPos += int64(n)
	}

	_ = workFile.Sync()
	config.ThroughputTime = time.Now().Sub(startTime)

	if config.Results != nil {
		config.Results.Lock()
		config.Results.WriteThroughput[config.WriterPath] = append(config.Results.WriteThroughput[config.WriterPath], &Throughput{ID: config.ID, Bytes: config.ThroughputBytes, Latencies: latencies, Time: config.ThroughputTime})
		config.Results.Unlock()
	}

	if Verbose {
		log.Printf(
			"[Writer %d] Wrote %0.2f MiB to %s (%0.2f MiB/sec, %0.2f sec.)\n",
			config.ID,
			float64(config.ThroughputBytes)/MiB,
			workFile.Name(),
			float64(config.ThroughputBytes)/MiB/config.ThroughputTime.Seconds(),
			config.ThroughputTime.Seconds(),
		)
	}
}

func prefill(filePath string, fileSize int64, wg *sync.WaitGroup) {
	var (
		bytesNeeded   int64
		data          []byte
		readerBufSize int = 33554432
		writeTotal    int64
	)

	defer wg.Done()

	data = make([]byte, readerBufSize)
	dr := NewDataReader(readerBufSize)

	workFile, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("%s: Error: %s.\n", filePath, err)
		return
	}
	defer workFile.Close()

	if Verbose {
		log.Printf("%s: Pre-filling starting.\n", filePath)
	}

	for {
		if Stop {
			break
		}
		if writeTotal >= fileSize {
			if Verbose {
				log.Printf("%s: Pre-filling complete.\n", filePath)
			}
			break
		}
		r, err := dr.Read(data)
		if err != nil || r < readerBufSize {
			log.Printf("%s: Data buffer filling failed. Read %d bytes, wanted %d.", filePath, r, readerBufSize)
			return
		}

		bytesNeeded = int64(len(data))
		if fileSize-writeTotal < int64(len(data)) {
			bytesNeeded = fileSize - writeTotal
		}

		n, err := workFile.Write(data[:bytesNeeded])
		if err != nil {
			_ = workFile.Close()
			log.Printf("%s: Error: %s\n", filePath, err)
			return
		}
		writeTotal += int64(n)
	}

	_ = workFile.Sync()
	_ = workFile.Close()
}
