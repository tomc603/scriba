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
	debug   bool
	verbose bool
)

func sizeHumanizer(f float64, base2 bool) string {
	const (
		base2kilo = 1 << 10
		base2mega = 1 << 20
		base2giga = 1 << 30
		base2tera = 1 << 40

		base10kilo = 1e3
		base10mega = 1e6
		base10giga = 1e9
		base10tera = 1e12
	)

	if base2 {
		switch {
		case f > base2tera:
			return fmt.Sprintf("%0.2f TB", f/base2tera)
		case f > base2giga:
			return fmt.Sprintf("%0.2f GB", f/base2giga)
		case f > base2mega:
			return fmt.Sprintf("%0.2f MB", f/base2mega)
		case f > base2kilo:
			return fmt.Sprintf("%0.2f KB", f/base2kilo)
		}
	} else {
		switch {
		case f > base10tera:
			return fmt.Sprintf("%0.2f TB", f/base10tera)
		case f > base10giga:
			return fmt.Sprintf("%0.2f GB", f/base10giga)
		case f > base10mega:
			return fmt.Sprintf("%0.2f MB", f/base10mega)
		case f > base10kilo:
			return fmt.Sprintf("%0.2f KB", f/base10kilo)
		}
	}

	// Whether we want base 10 or base 2, bytes are bytes.
	return fmt.Sprintf("%0.0f bytes", f)
}

func writer(id int, path string, outputSize int, flushSize int, keep bool, recordStats bool, results *writerResults, wg *sync.WaitGroup) {
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
	for writeTotal + len(data) <= outputSize {
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
	_ = tmpfile.Sync()
	_ = tmpfile.Close()

	results.Lock()
	results.d[path] = append(results.d[path], &throughput{writer: id, bytes: writeTotal, time: time.Now().Sub(startTime)})
	results.Unlock()

	if verbose {
		log.Printf(
			"[Writer %d] Wrote %s to %s (%s/s, %0.2f sec.)\n",
			id,
			sizeHumanizer(float64(writeTotal), true),
			tmpfile.Name(),
			sizeHumanizer(float64(writeTotal)/time.Now().Sub(startTime).Seconds(), true),
			time.Now().Sub(startTime).Seconds(),
		)
	}
}

func main() {
	var (
		cpuprofile  string
		keep        bool
		memprofile  string
		recordStats bool
		size        int
		writers     int
		flushSize   int
		wg          sync.WaitGroup
	)

	statsStopper := make(chan bool)
	stats := statsCollection{semaphore: statsStopper}

	results := new(writerResults)
	results.d = make(map[string][]*throughput)

	flag.StringVar(&cpuprofile, "cpuprofile", "", "Write the CPU profile to a file")
	flag.BoolVar(&debug, "debug", false, "Output debugging messages")
	flag.IntVar(&flushSize, "flush", 65536, "The amount of ata each writer should write before calling Sync")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.StringVar(&memprofile, "memprofile", "", "Write the memory profile to a file")
	flag.BoolVar(&recordStats, "stats", false, "Track block device IO statistics while testing")
	flag.IntVar(&size, "size", 32*1024*1024, "The target file size for each writer")
	flag.BoolVar(&verbose, "verbose", false, "Output extra running messages")
	flag.IntVar(&writers, "writers", 1, "The number of writer routines")
	flag.Parse()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\t%s [-debug] [-flush N] [-size N] [writers N] PATH [PATH...]\n\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "  PATH [PATH...]\n\tOne or more output paths for writers.")
	}

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "Error: You must specify at least one output path.\n")
		flag.Usage()
		os.Exit(1)
	}

	if cpuprofile != "" {
		cpuprofile_file, err := os.Create(cpuprofile)
		if err != nil {
			log.Panic(err)
		}
		pprof.StartCPUProfile(cpuprofile_file)
		defer pprof.StopCPUProfile()
	}

	// TODO: Sanity check the flushSize value

	writerID := 0
	if recordStats {
		go stats.CollectStats()
	}

	log.Printf("Starting %d writers\n", writers)
	for i := 0; i < writers; i++ {
		for _, pathValue := range flag.Args() {
			stats.Add(DevFromPath(pathValue))
			wg.Add(1)
			go writer(writerID, pathValue, size, flushSize, keep, recordStats, results, &wg)
			writerID++
		}
	}
	wg.Wait()

	pathThroughput := make(map[string]float64)
	for key, value := range results.d {
		for _, item := range value {
			pathThroughput[key] += float64(item.bytes) / item.time.Seconds()
			if verbose {
				log.Printf("%s: [%d]: %s/sec\n", key, item.writer, sizeHumanizer(float64(item.bytes)/item.time.Seconds(), true))
			}
		}
		log.Printf("%s: %s/sec\n", key, sizeHumanizer(pathThroughput[key], true))
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
