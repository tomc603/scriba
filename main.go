package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

type throughput struct {
	writer int
	bytes  int
	time   time.Duration
}

type writerResults struct {
	sync.Mutex
	d map[string][]*throughput
}

var (
	VersionMajor string = "0"
	VersionMinor string = "4"
	VersionBuild string
	debug        bool
	verbose      bool
)

func writer(id int, path string, outputSize int, flushSize int, keep bool, results *writerResults, wg *sync.WaitGroup) {
	var data []byte

	readerBufSize := 32 * 1024 * 1024
	writeTotal := 0

	data = make([]byte, flushSize)
	if flushSize <= 0 {
		data = make([]byte, readerBufSize)
	}

	defer wg.Done()

	if debug {
		log.Printf("[Writer %d] Generating random data buffer\n", id)
	}
	dr := NewDataReader(readerBufSize)
	if debug {
		log.Printf("[Writer %d] Generated %d random bytes", id, readerBufSize)
	}

	tmpfile, err := ioutil.TempFile(path, "output*.data")
	if err != nil {
		log.Printf("[Writer %d] Error: %s\n", id, err)
		return
	}
	if !keep {
		defer os.Remove(tmpfile.Name())
	}

	if debug {
		log.Printf("[Writer %d] Starting writer\n", id)
	}
	startTime := time.Now()
	for writeTotal+len(data) <= outputSize {
		r, err := dr.Read(data)
		if err != nil || r < flushSize {
			return
		}

		n, err := tmpfile.Write(data)
		if err != nil {
			_ = tmpfile.Close()
			log.Printf("[Writer %d] Error: %s\n", id, err)
			return
		}

		writeTotal += n
		if flushSize > 0 {
			_ = tmpfile.Sync()
		}
	}
	if writeTotal < outputSize {
		r, err := dr.Read(data)
		if err != nil || r < flushSize {
			return
		}

		n, err := tmpfile.Write(data[:outputSize-writeTotal])
		if err != nil {
			_ = tmpfile.Close()
			log.Printf("[Writer %d] Error: %s\n", id, err)
			return
		}
		writeTotal += n
	}
	if flushSize > 0 {
		_ = tmpfile.Sync()
	}
	_ = tmpfile.Close()

	results.Lock()
	results.d[path] = append(results.d[path], &throughput{writer: id, bytes: writeTotal, time: time.Now().Sub(startTime)})
	results.Unlock()

	if verbose {
		log.Printf(
			"[Writer %d] Wrote %0.2f to %s (%0.2f/s, %0.2f sec.)\n",
			id,
			float64(writeTotal) / MiB,
			tmpfile.Name(),
			float64(writeTotal)/MiB/time.Now().Sub(startTime).Seconds(),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}

func main() {
	var (
		cpuprofile  string
		flushSize   int
		keep        bool
		memprofile  string
		recordStats bool
		size        int
		version     bool
		wg          sync.WaitGroup
		writers     int
	)

	statsStopper := make(chan bool)
	stats := statsCollection{semaphore: statsStopper}

	results := new(writerResults)
	results.d = make(map[string][]*throughput)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\t%s [-debug] [-flush N] [-size N] [writers N] PATH [PATH...]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "  PATH [PATH...]\n\tOne or more output paths for writers.")
	}

	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write the CPU profile to a file")
	flag.BoolVar(&debug, "debug", false, "Output debugging messages")
	flag.IntVar(&flushSize, "flush", 65536, "The amount of ata each writer should write before calling Sync")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.StringVar(&memprofile, "memprofile", "", "Write the memory profile to a file")
	flag.BoolVar(&recordStats, "stats", false, "Track block device IO statistics while testing")
	flag.IntVar(&size, "size", 32*1024*1024, "The target file size for each writer")
	flag.BoolVar(&verbose, "verbose", false, "Output extra running messages")
	flag.BoolVar(&version, "version", false, "Output binary version and exit")
	flag.IntVar(&writers, "writers", 1, "The number of writer routines")
	flag.Parse()

	if cpuprofile != "" {
		cpuprofile_file, err := os.Create(cpuprofile)
		if err != nil {
			log.Panic(err)
		}
		pprof.StartCPUProfile(cpuprofile_file)
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

	writerID := 0
	if recordStats {
		go stats.CollectStats()
	}

	log.Printf("Starting %d writers\n", writers)
	for i := 0; i < writers; i++ {
		for _, pathValue := range flag.Args() {
			stats.Add(DevFromPath(pathValue))
			wg.Add(1)
			go writer(writerID, pathValue, size, flushSize, keep, results, &wg)
			writerID++
		}
	}
	wg.Wait()

	pathThroughput := make(map[string]float64)
	for key, value := range results.d {
		for _, item := range value {
			pathThroughput[key] += float64(item.bytes) / item.time.Seconds()
			if verbose {
				log.Printf("%s: [%d]: %0.2f/sec\n", key, item.writer, float64(item.bytes)/MiB/item.time.Seconds())
			}
		}
		log.Printf("%s: %0.2f/sec\n", key, pathThroughput[key]/MiB)
	}

	if recordStats {
		statsStopper <- true
		fmt.Printf("\nStats output:\n%s\n", stats.csv())
	}

	if memprofile != "" {
		memprofile_file, err := os.Create(memprofile)
		if err != nil {
			log.Panic(err)
		}
		pprof.Lookup("heap").WriteTo(memprofile_file, 2)
	}
}
