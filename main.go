package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TODO: For very long duration tests or extremely fast operations, we should batch write IO latency values
//const (
//	StatsWriteBatch int = 1000000
//)

var (
	Debug   bool
	Stop    bool
	Verbose bool
)

func setupSignalHandler(wc *[]*WriterConfig, rc *[]*ReaderConfig) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGTERM, os.Interrupt:
				log.Println("Received CTRL+C. Stopping routines.")
				Stop = true
				return
			case syscall.SIGUSR1:
				fmt.Print("Reader Throughput:\n")
				for _, v := range *rc {
					fmt.Printf("  [%d] %s: %s\n", v.ID, v.ReaderPath, humanizeSize(float64(v.ThroughputBytes), false))
				}

				fmt.Print("Writer Throughput:\n")
				for _, v := range *wc {
					fmt.Printf("  [%d] %s: %s\n", v.ID, v.WriterPath, humanizeSize(float64(v.ThroughputBytes), false))
				}
			default:
				log.Printf("ERROR: Received unhandled signal: %s\n", sig)
			}
		}
	}()
}

func main() {
	var (
		blockStats       SysStatsCollection
		cliBatchSize     int64
		cliBlockSize     int64
		cliBufferSize    int
		cliDirect        bool
		cliFileCount     int
		cliFileSize      int64
		cliIOLimit       int64
		cliPrefill       bool
		cliRecordStats   string
		cliRecordLatency string
		cliReadPattern   string
		cliReaders       int
		cliSeconds       int
		cliWritePattern  string
		cliWriters       int
		cliZero          bool
		ioFiles          []string
		ioPaths          []string
		ioStatsResults   *IOStats
		ioRunTime        time.Duration
		keep             bool
		randomMap        []int64
		readerConfigs    []*ReaderConfig
		readPattern      uint8
		version          bool
		wg               sync.WaitGroup
		writerConfigs    []*WriterConfig
		writePattern     uint8
	)

	statsStopper := make(chan bool)

	flag.Usage = func() {
		// TODO: If we can't output to Stderr, should we panic()?
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", path.Base(os.Args[0]))
		_, _ = fmt.Fprintf(os.Stderr, "\n\t%s [OPTIONS] PATH [PATH...]\n\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
		_, _ = fmt.Fprintln(os.Stderr, "  PATH [PATH...]\n\tOne or more output paths for writers.")
	}

	flag.BoolVar(&Debug, "debug", false, "Output debugging messages")
	flag.Int64Var(&cliBatchSize, "batch", 104857600, "The amount of data each writer should write before calling Sync")
	flag.Int64Var(&cliBlockSize, "block", 65536, "The size of each IO operation")
	flag.IntVar(&cliBufferSize, "buffer", 33554432, "Data buffer size for IO operations. Min: 65536.")
	flag.BoolVar(&cliDirect, "direct", false, "Linux only: Use direct file IO to skip filesystem cache. Default: false")
	flag.IntVar(&cliFileCount, "files", 1, "The number of files per path")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.BoolVar(&cliPrefill, "prefill", false, "Pre-fill files before performing IO tests.")
	flag.StringVar(&cliRecordLatency, "latency", "", "Save IO latency statistics to the specified path")
	flag.StringVar(&cliRecordStats, "stats", "", "Save block device IO statistics to the specified path")
	flag.StringVar(&cliReadPattern, "rpattern", "sequential", "The IO pattern for reader routines")
	flag.IntVar(&cliReaders, "readers", 0, "The number of reader routines")
	flag.IntVar(&cliSeconds, "time", 0, "The number of seconds to run IO routines. Overrides total value")
	flag.Int64Var(&cliFileSize, "size", 33554432, "The target file size for each IO routine")
	flag.Int64Var(&cliIOLimit, "total", 33554432, "The total amount of data to read and write per file")
	flag.BoolVar(&Verbose, "verbose", false, "Output extra running messages")
	flag.BoolVar(&version, "version", false, "Output binary version and exit")
	flag.StringVar(&cliWritePattern, "wpattern", "sequential", "The IO pattern for writer routines")
	flag.IntVar(&cliWriters, "writers", 1, "The number of writer routines")
	flag.BoolVar(&cliZero, "zeroes", false, "Use zero data instead of pseudo-random data.")
	flag.Parse()

	if Debug {
		// If we've enabled debug output, then we should obviously output verbose messaging too.
		Verbose = true
	}

	if version {
		fmt.Printf("%s\nVersion   : %s.%s.%s\nBuild Tag : %s\nBuild Date: %s\n", path.Base(os.Args[0]), VersionMajor, VersionMinor, VersionPoint, VersionTag, BuildDate)
		os.Exit(0)
	}

	if cliDirect {
		osname := runtime.GOOS
		if osname != "Linux" {
			log.Println("ERROR: Direct I/O is only supported by Linux.")
			os.Exit(1)
		}
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
	ioPaths = uniquePaths(flag.Args())

	cliReadPattern = strings.ToLower(cliReadPattern)
	switch cliReadPattern {
	case "random":
		readPattern = Random
	case "repeat":
		readPattern = Repeat
	case "sequential":
		readPattern = Sequential
	default:
		log.Printf("ERROR: Read pattern must be random, repeat, or sequential. %s is invalid.\n", cliReadPattern)
		os.Exit(1)
	}

	cliWritePattern = strings.ToLower(cliWritePattern)
	switch cliWritePattern {
	case "random":
		writePattern = Random
	case "repeat":
		writePattern = Repeat
	case "sequential":
		writePattern = Sequential
	case "zipf":
		writePattern = Zipf
	default:
		log.Printf("ERROR: Read pattern must be random, repeat, sequential, or zipf. %s is invalid.\n", cliWritePattern)
		os.Exit(1)
	}

	if writePattern == Random || readPattern == Random {
		randomMap = make([]int64, cliFileSize/cliBlockSize)
		if Debug {
			log.Printf("Populating %d random map entries.\n", len(randomMap))
		}
		for k := range randomMap {
			randomMap[k] = int64(k)
		}
		if Debug {
			log.Println("Shuffling random map entries.")
		}
		rand.Shuffle(len(randomMap), func(i, j int) {
			randomMap[i], randomMap[j] = randomMap[j], randomMap[i]
		})
	}

	if cliRecordStats != "" && runtime.GOOS != "linux" {
		log.Println("WARNING: Recording block IO stats is only supported on Linux. Disabling.")
		cliRecordStats = ""
	}

	if cliBlockSize < 4096 {
		log.Println("WARNING: Block sizes below 4k are probably nonsense to test.")
	}

	if cliRecordStats != "" {
		if fInfo, fErr := os.Stat(cliRecordStats); os.IsNotExist(fErr) {
			log.Printf("ERROR: IO stats path %s does not exist.\n", cliRecordStats)
			os.Exit(1)
		} else if fErr != nil && !os.IsNotExist(fErr) {
			log.Printf("ERROR: Unable to access IO stats path %s. %s\n", cliRecordStats, fErr)
			os.Exit(1)
		} else if !fInfo.IsDir() {
			log.Println("ERROR: IO stats path is not a directory.")
			os.Exit(1)
		}

		blockStats = SysStatsCollection{Semaphore: statsStopper}
	}

	if cliRecordLatency != "" {
		if fInfo, fErr := os.Stat(cliRecordLatency); os.IsNotExist(fErr) {
			log.Printf("ERROR: Latency stats path %s does not exist.\n", cliRecordLatency)
			os.Exit(1)
		} else if fErr != nil && !os.IsNotExist(fErr) {
			log.Printf("ERROR: Unable to access latency stats path %s. %s\n", cliRecordLatency, fErr)
			os.Exit(1)
		} else if !fInfo.IsDir() {
			log.Println("ERROR: Latency stats path is not a directory.")
			os.Exit(1)
		}

		log.Println("Setting up latency struct")
		ioStatsResults = new(IOStats)
		ioStatsResults.ReadThroughput = make(map[string][]*Throughput)
		ioStatsResults.WriteThroughput = make(map[string][]*Throughput)
	}

	ioRunTime = 0
	if cliSeconds > 0 {
		if Debug {
			log.Println("Enabling timed run, setting IO limit to zero.")
		}
		ioRunTime = time.Second * time.Duration(cliSeconds)
		cliIOLimit = 0
	}

	// Wait for CTRL+C in the background
	setupSignalHandler(&writerConfigs, &readerConfigs)

	log.Println("Creating files")
	for _, ioPath := range ioPaths {
		if Stop {
			os.Exit(0)
		}
		// Add the path to the IO stats collector list
		if cliRecordStats != "" {
			blockStats.Add(DevFromPath(ioPath))
		}

		for j := 0; j < cliFileCount; j++ {
			// Since we can't create files on raw devices, just use the raw device
			// TODO: Add generic block device detection
			if ioPath == "/dev/null" || ioPath == "/dev/zero" {
				ioFiles = append(ioFiles, ioPath)
				continue
			}

			filePath := path.Join(ioPath, fmt.Sprintf("scriba.%d.data", j))
			if Verbose {
				log.Printf("Allocating %s\n", filePath)
			}

			if allocErr := Allocate(filePath, cliFileSize, keep); allocErr != nil {
				log.Printf("ERROR: Unable to allocate %s. %s", filePath, allocErr)
				os.Exit(2)
			}
			if cliPrefill {
				wg.Add(1)
				go prefill(filePath, cliFileSize, cliZero, &wg)
			}

			ioFiles = append(ioFiles, filePath)
		}
	}
	wg.Wait()
	dropPageCache()

	if cliRecordStats != "" {
		go blockStats.CollectStats()
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
					BufferSize:  cliBufferSize,
					Direct:      cliDirect,
					FileSize:    cliFileSize,
					RandomMap:   &randomMap,
					StartOffset: cliFileSize / int64(cliWriters) * int64(writerID),
					WriteLimit:  cliIOLimit,
					WriteTime:   ioRunTime,
					WriterPath:  ioFile,
					WriterType:  writePattern,
					Zeroes:      cliZero,
					Results:     ioStatsResults,
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
				log.Printf("[%s] Starting %d readers\n", ioFile, cliReaders)
			}
			readerID := 0
			for i := 0; i < cliReaders; i++ {
				rc := ReaderConfig{
					ID:          readerID,
					BlockSize:   cliBlockSize,
					Direct:      cliDirect,
					FileSize:    cliFileSize,
					RandomMap:   &randomMap,
					ReadLimit:   cliIOLimit,
					ReadTime:    ioRunTime,
					ReaderPath:  ioFile,
					ReaderType:  readPattern,
					Results:     ioStatsResults,
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
		if Verbose {
			log.Println("Cleaning up test files.")
		}
		for _, ioFile := range ioFiles {
			if ioFile == "/dev/null" || ioFile == "/dev/zero" {
				continue
			}
			if err := os.Remove(ioFile); err != nil {
				log.Printf("ERROR: Unable to delete %s. %s\n", ioFile, err)
			}
		}
	}

	if cliRecordStats != "" {
		if Verbose {
			log.Println("Saving block IO stats")
		}
		statsStopper <- true

		if err := blockStats.Write(cliRecordStats); err != nil {
			log.Printf("ERROR: An error occurred saving block IO stats. %s\n", err)
		}
	}

	// Output reader routine throughputs
	fmt.Println("Reader performance:")
	//pathThroughputTotals := make(map[string]float64)
	pathThroughputGrandTotal := 0.0
	for _, rc := range readerConfigs {
		fmt.Printf("[%d] %s: %0.2f MiB/sec.\n", rc.ID, rc.ReaderPath, float64(rc.ThroughputBytes)/MiB/rc.ThroughputTime.Seconds())
		//pathThroughputTotals[rc.ReaderPath] = float64(rc.ThroughputBytes) / MiB / rc.ThroughputTime.Seconds()
		pathThroughputGrandTotal += float64(rc.ThroughputBytes) / MiB / rc.ThroughputTime.Seconds()
		//for _, v := range ioPaths {
		//	if strings.HasPrefix(rc.ReaderPath, v) {
		//		pathThroughputTotals[v] += float64(rc.ThroughputBytes) / MiB / rc.ThroughputTime.Seconds()
		//	}
		//}
	}
	//for k, v := range pathThroughputTotals {
	//	fmt.Printf("%s: %0.2f MiB/sec.\n", k, v)
	//}
	fmt.Printf("Read Total: %0.2f MiB/sec.\n", pathThroughputGrandTotal)

	// Output writer routine throughputs
	fmt.Println("Writer performance:")
	//pathThroughputTotals = make(map[string]float64)
	pathThroughputGrandTotal = 0.0
	for _, wc := range writerConfigs {
		fmt.Printf("[%d] %s: %0.2f MiB/sec.\n", wc.ID, wc.WriterPath, float64(wc.ThroughputBytes)/MiB/wc.ThroughputTime.Seconds())
		//pathThroughputTotals[wc.WriterPath] = float64(wc.ThroughputBytes) / MiB / wc.ThroughputTime.Seconds()
		pathThroughputGrandTotal += float64(wc.ThroughputBytes) / MiB / wc.ThroughputTime.Seconds()
		//for _, v := range ioPaths {
		//	//if strings.HasPrefix(wc.WriterPath, v) {
		//	//	pathThroughputTotals[v] += float64(wc.ThroughputBytes) / MiB / wc.ThroughputTime.Seconds()
		//	//}
		//}
	}
	//for k, v := range pathThroughputTotals {
	//	fmt.Printf("%s: %0.2f MiB/sec.\n", k, v)
	//}
	fmt.Printf("Write Total: %0.2f MiB/sec.\n", pathThroughputGrandTotal)

	if cliRecordLatency != "" && ioStatsResults != nil {
		if Verbose {
			log.Println("Saving latency stats")
		}
		if err := ioStatsResults.Write(cliRecordLatency); err != nil {
			log.Printf("ERROR: Unable to save IO latency stats. %s\n", err)
		}
	}
}
