package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	StatsWriteBatch int = 1000000
)

var (
	VersionMajor string = "0"
	VersionMinor string = "10"
	VersionBuild string
	Debug        bool
	Verbose      bool
	//ratio_count   uint64
)

func main() {
	var (
		cliBatchSize      int64
		cliBlockSize      int64
		cliCPUProfilePath string
		cliFileCount      int
		cliFileSize       int64
		cliIOLimit        int64
		cliRecordStats    string
		cliRecordLatency  string
		cliReaders        int
		cliSeconds        int
		ioFiles           []string
		ioPaths           []string
		ioRunTime         time.Duration
		keep              bool
		readerConfigs     []*ReaderConfig
		version           bool
		wg                sync.WaitGroup
		writerConfigs     []*WriterConfig
		cliWriters        int
	)

	statsStopper := make(chan bool)
	stats := statsCollection{semaphore: statsStopper}

	readResults := new(writerResults)
	readResults.d = make(map[string][]*throughput)

	writeResults := new(writerResults)
	writeResults.d = make(map[string][]*throughput)

	flag.Usage = func() {
		// TODO: If we can't output to Stderr, should we panic()?
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		_, _ = fmt.Fprintf(os.Stderr, "\n\t%s [OPTIONS] PATH [PATH...]\n\n", os.Args[0])
		flag.PrintDefaults()
		_, _ = fmt.Fprintln(os.Stderr, "  PATH [PATH...]\n\tOne or more output paths for writers.")
	}

	flag.StringVar(&cliCPUProfilePath, "cpuprofile", "", "Write a CPU profile to a file")
	flag.BoolVar(&Debug, "debug", false, "Output debugging messages")
	flag.Int64Var(&cliBatchSize, "batch", 104857600, "The amount of data each writer should write before calling Sync")
	flag.Int64Var(&cliBlockSize, "block", 65536, "The size of each IO operation")
	flag.IntVar(&cliFileCount, "files", 1, "The number of files per path")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.StringVar(&cliRecordLatency, "latency", "", "Save IO latency statistics to the specified path")
	flag.StringVar(&cliRecordStats, "stats", "", "Save block device IO statistics to the specified path")
	flag.IntVar(&cliReaders, "readers", 0, "The number of reader routines")
	flag.IntVar(&cliSeconds, "time", 0, "The number of seconds to run IO routines. Overrides total value")
	flag.Int64Var(&cliFileSize, "size", 33554432, "The target file size for each IO routine")
	flag.Int64Var(&cliIOLimit, "total", 33554432, "The total amount of data to read and write per file")
	flag.BoolVar(&Verbose, "verbose", false, "Output extra running messages")
	flag.BoolVar(&version, "version", false, "Output binary version and exit")
	flag.IntVar(&cliWriters, "writers", 1, "The number of writer routines")
	flag.Parse()

	if Debug {
		// If we've enabled debug output, then we should obviously output verbose messaging too.
		Verbose = true
	}

	if cliCPUProfilePath != "" {
		cpuProfileFile, err := os.Create(cliCPUProfilePath)
		if err != nil {
			log.Panic(err)
		}
		if err := pprof.StartCPUProfile(cpuProfileFile); err != nil {
			log.Printf("ERROR: Unable to start CPU profiler. %s\n", err)
		} else {
			defer pprof.StopCPUProfile()
		}
	}

	if version {
		fmt.Printf("%s version %s.%s.%s\n", os.Args[0], VersionMajor, VersionMinor, VersionBuild)
		os.Exit(0)
	}

	if cliReaders == 0 && cliWriters == 0 {
		log.Println("ERROR: At least 1 reader or writer must be executed.")
		os.Exit(1)

	}

	if cliFileSize < 1 {
		log.Println("ERROR: Invalid file size specified. File sizes must be greater than 0 bytes.")
		os.Exit(1)
	}

	if cliIOLimit < 1 && cliSeconds < 1 {
		log.Println("ERROR: A seconds or total must be greater than 0.")
		os.Exit(1)
	}

	if len(flag.Args()) == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "ERROR: You must specify at least one output path.\n")
		flag.Usage()
		os.Exit(1)
	}
	ioPaths = flag.Args()

	if cliRecordStats != "" && runtime.GOOS != "linux" {
		log.Println("WARNING: Recording block IO stats is only supported on Linux. Disabling.")
		cliRecordStats = ""
	}

	if cliBlockSize < 4096 {
		log.Println("WARNING: Block sizes below 4k are probably nonsense to test.")
	}

	ioRunTime = 0
	if cliSeconds > 0 {
		if Debug {
			log.Println("Enabling timed run, setting IO limit to zero.")
		}
		ioRunTime = time.Second * time.Duration(cliSeconds)
		cliIOLimit = 0
	}

	log.Println("Creating files")
	for i, ioPath := range ioPaths {
		// Add the path to the IO stats collector list
		stats.Add(DevFromPath(ioPath))

		for j := 0; j < cliFileCount; j++ {
			if ioPath == "/dev/null" || ioPath == "/dev/zero" {
				ioFiles = append(ioFiles, ioPath)
				continue
			}

			filePath := path.Join(ioPath, fmt.Sprintf("scriba.path_%d.file_%d.data", i, j))
			if Verbose {
				log.Printf("Allocating %s\n", filePath)
			}
			if alloc_err := Allocate(filePath, cliFileSize); alloc_err != nil {
				log.Printf("ERROR: Unable to allocate %s. %s", filePath, alloc_err)
				os.Exit(2)
			}
			ioFiles = append(ioFiles, filePath)
		}
	}

	if cliRecordStats != "" {
		go stats.CollectStats()
	}

	log.Println("Starting io routines")
	for _, ioFile := range ioFiles {
		if ioFile != "/dev/zero" {
			if Verbose {
				log.Printf("[%s] Starting %d writers\n", ioFile, cliWriters)
			}
			writerID := 0
			for i := 0; i < cliWriters; i++ {
				wc := WriterConfig{
					ID:          writerID,
					BatchSize:   cliBatchSize,
					BlockSize:   cliBlockSize,
					FileSize:    cliFileSize,
					StartOffset: cliFileSize / int64(cliWriters) * int64(writerID),
					WriteLimit:  cliIOLimit,
					WriteTime:   ioRunTime,
					WriterPath:  ioFile,
					WriterType:  Sequential,
					Results:     writeResults,
				}
				writerConfigs = append(writerConfigs, &wc)
				wg.Add(1)
				go writer(&wc, &wg)
				writerID++
			}
		} else {
			log.Println("Skipping writers for /dev/zero")
		}

		if ioFile != "/dev/null" {
			if Verbose {
				log.Printf("[%s] Starting %d cliReaders\n", ioFile, cliReaders)
			}
			readerID := 0
			for i := 0; i < cliReaders; i++ {
				rc := ReaderConfig{
					ID:          readerID,
					BlockSize:   cliBlockSize,
					ReadLimit:   cliIOLimit,
					ReadTime:    ioRunTime,
					ReaderPath:  ioFile,
					ReaderType:  Sequential,
					Results:     readResults,
					StartOffset: cliFileSize / int64(cliReaders) * int64(readerID),
				}
				readerConfigs = append(readerConfigs, &rc)
				wg.Add(1)
				go reader(&rc, &wg)
				readerID++
			}
		} else {
			log.Println("Skipping readers for /dev/null")
		}
	}
	wg.Wait()

	if !keep {
		for _, ioFile := range ioFiles {
			if ioFile == "/dev/null" || ioFile == "/dev/zero" {
				continue
			}
			if err := os.Remove(ioFile); err != nil {
				log.Printf("ERROR: Unable to delete %s. %s\n", ioFile, err)
			}
		}
	}

	log.Println("Writer Performance")
	pathWriteThroughput := make(map[string]float64)
	for key, value := range writeResults.d {
		for _, item := range value {
			pathWriteThroughput[key] += float64(item.bytes) / item.time.Seconds()
			if Verbose {
				for _, latency := range item.latencies {
					log.Printf(
						"%s: [%d]: %d us",
						key, item.id, latency.Microseconds(),
					)
				}
			}
			log.Printf(
				"%s: [%d]: %0.2f MiB/sec, Min: %d us, Max: %d us, Avg: %d us, P50: %d us, P95: %d us, P99: %d us\n",
				key, item.id, float64(item.bytes)/MiB/item.time.Seconds(), item.Min().Microseconds(), item.Max().Microseconds(),
				item.Average().Microseconds(), item.Percentile(0.50).Microseconds(),
				item.Percentile(0.95).Microseconds(), item.Percentile(0.99).Microseconds(),
			)
		}
		log.Printf("%s: %0.2f MiB/sec\n", key, pathWriteThroughput[key]/MiB)
	}

	log.Println("Reader Performance")
	pathReadThroughput := make(map[string]float64)
	for key, value := range readResults.d {
		for _, item := range value {
			pathReadThroughput[key] += float64(item.bytes) / item.time.Seconds()
			if Verbose {
				for _, latency := range item.latencies {
					log.Printf(
						"%s: [%d]: %d us",
						key, item.id, latency.Microseconds(),
					)
				}
			}
			log.Printf(
				"%s: [%d]: %0.2f MiB/sec, Min: %d us, Max: %d us, Avg: %d us, P50: %d us, P95: %d us, P99: %d us\n",
				key, item.id, float64(item.bytes)/MiB/item.time.Seconds(), item.Min().Microseconds(), item.Max().Microseconds(),
				item.Average().Microseconds(), item.Percentile(0.50).Microseconds(),
				item.Percentile(0.95).Microseconds(), item.Percentile(0.99).Microseconds(),
			)
		}
		log.Printf("%s: %0.2f MiB/sec\n", key, pathReadThroughput[key]/MiB)
	}

	// log.Printf("Write iterations: %d\n", ratio_count)

	if cliRecordLatency != "" {
		if Verbose {
			log.Println("Saving latency stats")
		}

		latencyFile, err := os.Open(cliRecordLatency)
		if err != nil {
			log.Printf("ERROR: Unable to open block IO stats file %s. %s\n", cliRecordStats, err)
		}
		if _, err := latencyFile.WriteString(stats.csv()); err != nil {
			log.Printf("ERROR: An error occurred writing stats log. %s\n", err)
		}
		_ = latencyFile.Sync()
		if err := latencyFile.Close(); err != nil {
			log.Printf("ERROR: An error occurred closing the stats log. %s\n", err)
		}
	}

	if cliRecordStats != "" {
		if Verbose {
			log.Println("Saving block IO stats")
		}
		statsStopper <- true

		statsFile, err := os.Open(cliRecordStats)
		if err != nil {
			log.Printf("ERROR: Unable to open block IO stats file %s. %s\n", cliRecordStats, err)
		}
		if _, err := statsFile.WriteString(stats.csv()); err != nil {
			log.Printf("ERROR: An error occurred writing stats log. %s\n", err)
		}
		_ = statsFile.Sync()
		if err := statsFile.Close(); err != nil {
			log.Printf("ERROR: An error occurred closing the stats log. %s\n", err)
		}
	}
}
