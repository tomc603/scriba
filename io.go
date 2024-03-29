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
	Zipf
)

type ReaderConfig struct {
	BlockSize       int64
	BytePattern     int
	Direct          bool
	FileSize        int64
	ID              int
	RandomMap       *[]int64
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
	BufferSize      int
	BytePattern     int
	Direct          bool
	FileSize        int64
	ID              int
	RandomMap       *[]int64
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
		mapIndex     int
		seek         bool
		seekPosition int64
	)

	buf := make([]byte, config.BlockSize)
	if len(*config.RandomMap) > 0 {
		mapIndex = rand.Intn(len(*config.RandomMap))
	}

	defer wg.Done()

	workFile, err := os.OpenFile(config.ReaderPath, readerFlags(config.Direct), 0666)
	if err != nil {
		log.Printf("[Reader %d] Error opening file %s: %s\n", config.ID, config.ReaderPath, err)
		return
	}
	defer func(workFile *os.File) {
		err := workFile.Close()
		if err != nil {
			log.Fatalf("[Reader %d] Unable to close file %s. %s", config.ID, workFile.Name(), err)
		}
	}(workFile)

	if off, err := workFile.Seek(config.StartOffset, 0); err != nil {
		log.Printf("[Reader %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.ReaderPath, config.StartOffset, err)
	} else {
		if Debug {
			log.Printf("[Reader %d] New offset %s@%d", config.ID, config.ReaderPath, off)
		}
	}

	if Debug {
		log.Printf("[Reader %d] Starting reader\n", config.ID)
	}

	startTime := time.Now()
	for {
		seek = false

		if Stop {
			// The user has interrupted us, so stop reading and return normally.
			break
		}
		if config.ReadLimit > 0 && config.ThroughputBytes >= config.ReadLimit {
			// A data limit has been specified, and we've reached or exceeded it.
			if Verbose {
				log.Printf("[Reader %d]: Data limit has elapsed. Stopping reader routine.\n", config.ID)
			}
			break
		}
		if config.ReadTime > 0 && time.Now().Sub(startTime) >= config.ReadTime {
			// A time limit has been specified, and we've reached or exceeded it.
			if Verbose {
				log.Printf("[Reader %d]: Time limit has elapsed. Stopping reader routine.\n", config.ID)
			}
			break
		}
		bytesToRead = config.BlockSize
		if config.ReadLimit > 0 && config.ReadLimit-config.ThroughputBytes < bytesToRead {
			bytesToRead = config.ReadLimit - config.ThroughputBytes
		}

		// If we aren't performing sequential I/O, calculate the position to seek for the next operation
		if config.ReaderType != Sequential {
			seek = true
			seekPosition = config.StartOffset

			if config.ReaderType == Random {
				mapIndex += 1
				if mapIndex > len(*config.RandomMap)-1 {
					mapIndex = 0
				}
				seekPosition = (*config.RandomMap)[mapIndex]
				//seekPosition = rand.Int63n(config.FileSize - bytesToRead)
			}
		}

		// Calculate latency only after the new position is determined.
		latencyStart := time.Now()

		if seek {
			if _, err := workFile.Seek(seekPosition, 0); err != nil {
				log.Printf("[Reader %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.ReaderPath, seekPosition, err)
				return
			}
		}

		n, readErr := workFile.Read(buf[:bytesToRead])
		config.ThroughputBytes += int64(n)
		if readErr != nil {
			if readErr == io.EOF {
				// We might read a partial buffer here, but detecting EOF and
				// seeking to the beginning is the easiest way to continue reading
				// while we need more data. This should be extremely rare.
				if Debug {
					log.Printf("[Reader %d]: Reached EOF, seeking to 0\n", config.ID)
				}
				if _, err := workFile.Seek(0, 0); err != nil {
					log.Printf("[Reader %d]: ERROR Unable to seek to beginning of %s. %s\n", config.ID, config.ReaderPath, readErr)
				}
				continue
			}
			log.Printf("[Reader %d] ERROR: Unable to read from %s. %v\n", config.ID, workFile.Name(), readErr)
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
		config.Results.ReadThroughput[config.ReaderPath] = append(config.Results.ReadThroughput[config.ReaderPath], &Throughput{ID: config.ID, Latencies: latencies})
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
		mapIndex     int
		seek         bool
		seekPosition int64
	)

	readerBufSize := config.BufferSize
	data = make([]byte, config.BlockSize)
	if len(*config.RandomMap) > 0 {
		mapIndex = rand.Intn(len(*config.RandomMap))
	}

	defer wg.Done()

	if Debug {
		log.Printf("[Writer %d] Generating random data buffer\n", config.ID)
	}
	dr := NewDataReader(readerBufSize, config.BytePattern)
	if Debug {
		log.Printf("[Writer %d] Generated %d random bytes", config.ID, readerBufSize)
	}

	workFile, openError := os.OpenFile(config.WriterPath, writerFlags(config.Direct), 0644)
	if openError != nil {
		log.Printf("[Writer %d] Error: %s\n", config.ID, openError)
		return
	}
	defer func(workFile *os.File) {
		err := workFile.Close()
		if err != nil {
			log.Fatalf("Unable to close file %s. %s", workFile.Name(), err)
		}
	}(workFile)

	if off, seekError := workFile.Seek(config.StartOffset, 0); seekError != nil {
		log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, config.StartOffset, seekError)
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
			if Verbose {
				log.Printf("[Writer %d]: Data limit has elapsed. Stopping writer routine.\n", config.ID)
			}
			break
		}
		if config.WriteTime != 0 && time.Now().Sub(startTime) >= config.WriteTime {
			// A time limit has been specified, and we've reached or exceeded that limit.
			if Verbose {
				log.Printf("[Writer %d]: Time limit has elapsed. Stopping writer routine.\n", config.ID)
			}
			break
		}
		r, dataReadError := dr.Read(data)
		if dataReadError != nil || int64(r) < config.BlockSize {
			return
		}

		bytesNeeded = int64(len(data))
		if config.WriteLimit > 0 && config.WriteLimit-config.ThroughputBytes < int64(len(data)) {
			bytesNeeded = config.WriteLimit - config.ThroughputBytes
		}

		if config.WriterType != Sequential {
			seek = true
			seekPosition = config.StartOffset

			if config.WriterType == Random {
				mapIndex += 1
				if mapIndex > len(*config.RandomMap)-1 {
					mapIndex = 0
				}
				seekPosition = (*config.RandomMap)[mapIndex]
				//seekPosition = rand.Int63n(config.FileSize - config.BlockSize)
			}
		}

		if lastPos+bytesNeeded > config.FileSize {
			if Debug {
				log.Printf("[Writer %d] %s EOF, seeking to 0", config.ID, config.WriterPath)
			}
			seekPosition = 0
			seek = true
		}

		// Don't count the position calculation in the latency math
		latencyStart := time.Now()
		if seek {
			if seekPos, seekError := workFile.Seek(seekPosition, 0); seekError != nil {
				log.Printf("[Writer %d] ERROR: Unable to seek %s@%d. %s\n", config.ID, config.WriterPath, seekPosition, seekError)
			} else {
				lastPos = seekPos
			}
		}

		n, writeError := workFile.Write(data[:bytesNeeded])
		if writeError != nil {
			_ = workFile.Close()
			log.Printf("[Writer %d] Error: %s\n", config.ID, writeError)
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
		config.Results.WriteThroughput[config.WriterPath] = append(config.Results.WriteThroughput[config.WriterPath], &Throughput{ID: config.ID, Latencies: latencies})
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

func prefill(filePath string, fileSize int64, pattern int, wg *sync.WaitGroup) {
	var (
		bytesNeeded int64
		data        []byte
		writeTotal  int64
	)
	readerBufSize := 33554432

	defer wg.Done()

	data = make([]byte, readerBufSize)
	dr := NewDataReader(readerBufSize, pattern)

	//writerFlags(config.Direct)
	//workFile, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	workFile, err := os.OpenFile(filePath, writerFlags(true), 0644)
	if err != nil {
		log.Printf("%s: Error: %s.\n", filePath, err)
		return
	}
	defer func(workFile *os.File) {
		err := workFile.Close()
		if err != nil {
			log.Fatalf("Unable to close file %s. %s", workFile.Name(), err)
		}
	}(workFile)

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
}
