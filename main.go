package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"sync"
)

var (
	VersionMajor string = "0"
	VersionMinor string = "7"
	VersionBuild string
	Debug        bool
	Verbose      bool
	//ratio_count  uint64
)

func main() {
	var (
		cpuprofile    string
		flushSize     int64
		ioFiles       []string
		ioPaths       []string
		keep          bool
		recordStats   bool
		readerConfigs []*ReaderConfig
		readers       int
		size          int64
		version       bool
		wg            sync.WaitGroup
		writerConfigs []*WriterConfig
		writers       int
	)

	statsStopper := make(chan bool)
	stats := statsCollection{semaphore: statsStopper}

	readResults := new(writerResults)
	readResults.d = make(map[string][]*throughput)

	writeResults := new(writerResults)
	writeResults.d = make(map[string][]*throughput)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\t%s [-debug] [-flush N] [-size N] [writers N] PATH [PATH...]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "  PATH [PATH...]\n\tOne or more output paths for writers.")
	}

	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write the CPU profile to a file")
	flag.BoolVar(&Debug, "debug", false, "Output debugging messages")
	flag.Int64Var(&flushSize, "flush", 65536, "The amount of ata each writer should write before calling Sync")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.BoolVar(&recordStats, "stats", false, "Track block device IO statistics while testing")
	flag.IntVar(&readers, "readers", 0, "The number of reader routines")
	flag.Int64Var(&size, "size", 32*1024*1024, "The target file size for each writer")
	flag.BoolVar(&Verbose, "verbose", false, "Output extra running messages")
	flag.BoolVar(&version, "version", false, "Output binary version and exit")
	flag.IntVar(&writers, "writers", 1, "The number of writer routines")
	flag.Parse()

	if Debug {
		// If we've enabled debug output, then we should obviously output verbose messaging too.
		Verbose = true
	}

	if cpuprofile != "" {
		cpuProfileFile, err := os.Create(cpuprofile)
		if err != nil {
			log.Panic(err)
		}
		pprof.StartCPUProfile(cpuProfileFile)
		defer pprof.StopCPUProfile()
	}

	if version {
		fmt.Printf("%s version %s.%s.%s\n", os.Args[0], VersionMajor, VersionMinor, VersionBuild)
		os.Exit(0)
	}

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "Error: You must specify at least one output path.\n")
		flag.Usage()
		os.Exit(1)
	}
	ioPaths = flag.Args()

	log.Println("Creating files")
	for i, ioPath := range ioPaths {
		// Add the path to the IO stats collector list
		stats.Add(DevFromPath(ioPath))

		filePath := ioPath
		if ioPath != "/dev/null" && ioPath != "/dev/zero" {
			filePath = path.Join(ioPath, fmt.Sprintf("scriba.%d.data", i))

			log.Printf("Allocating %s\n", filePath)
			if f, err := os.Create(filePath); err != nil {
				log.Printf("ERROR: Unable to create %s. %s", filePath, err)
				os.Exit(2)
			} else {
				if err := f.Truncate(size); err != nil {
					log.Printf("ERROR: Unable to allocate %s. %s", filePath, err)
					os.Exit(2)
				}
				_ = f.Close()
			}
		}
		ioFiles = append(ioFiles, filePath)
	}

	if recordStats {
		go stats.CollectStats()
	}

	log.Println("Starting io routines")
	for _, ioFile := range ioFiles {
		if ioFile != "/dev/zero" {
			if Verbose {
				log.Printf("[%s] Starting %d writers\n", ioFile, writers)
			}
			writerID := 0
			for i := 0; i < writers; i++ {
				wc := WriterConfig{
					ID:          writerID,
					FlushSize:   flushSize,
					Keep:        keep,
					OutputSize:  size,
					StartOffset: size / int64(writers) * int64(writerID),
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
				log.Printf("[%s] Starting %d readers\n", ioFile, readers)
			}
			readerID := 0
			for i := 0; i < readers; i++ {
				rc := ReaderConfig{
					ID:          readerID,
					BlockSize:   flushSize,
					TotalSize:   size,
					ReaderPath:  ioFile,
					ReaderType:  Sequential,
					Results:     readResults,
					StartOffset: size / int64(readers) * int64(readerID),
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

	if recordStats {
		statsStopper <- true
		fmt.Printf("\nStats output:\n%s\n", stats.csv())
	}
}
