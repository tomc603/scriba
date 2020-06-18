package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

type throughput struct {
	id        int
	bytes     uint64
	latencies []time.Duration
	time      time.Duration
}

type writerResults struct {
	sync.Mutex
	d map[string][]*throughput
}

var (
	VersionMajor string = "0"
	VersionMinor string = "6"
	VersionBuild string
	Debug        bool
	Verbose      bool
	//ratio_count  uint64
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

func median(values []float64) float64 {
	middle := len(values) / 2
	sort.Float64s(values)
	if len(values)%2 == 0 {
		return values[middle-1] + values[middle+1]/2
	}
	return values[middle]
}

func main() {
	var (
		cpuprofile    string
		flushSize     uint64
		keep          bool
		memprofile    string
		recordStats   bool
		size          uint64
		version       bool
		wg            sync.WaitGroup
		writerConfigs []*WriterConfig
		writers       int
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
	flag.BoolVar(&Debug, "debug", false, "Output debugging messages")
	flag.Uint64Var(&flushSize, "flush", 65536, "The amount of ata each writer should write before calling Sync")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.StringVar(&memprofile, "memprofile", "", "Write the memory profile to a file")
	flag.BoolVar(&recordStats, "stats", false, "Track block device IO statistics while testing")
	flag.Uint64Var(&size, "size", 32*1024*1024, "The target file size for each writer")
	flag.BoolVar(&Verbose, "verbose", false, "Output extra running messages")
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

	if recordStats {
		go stats.CollectStats()
	}

	log.Printf("Starting %d writers\n", writers)
	writerID := 0
	for i := 0; i < writers; i++ {
		for _, pathValue := range flag.Args() {
			wc := WriterConfig{
				ID:         writerID,
				FlushSize:  flushSize,
				Keep:       keep,
				OutputSize: size,
				WriterPath: pathValue,
				WriterType: Sequential,
				Results:    results,
				wg:         &wg,
			}
			writerConfigs = append(writerConfigs, &wc)
			stats.Add(DevFromPath(pathValue))
			wg.Add(1)
			go writer(&wc)
			writerID++
		}
	}
	wg.Wait()

	pathThroughput := make(map[string]float64)
	for key, value := range results.d {
		for _, item := range value {
			pathThroughput[key] += float64(item.bytes) / item.time.Seconds()
			if Verbose {
				pathLatency := int64(0)
				for _, latency := range item.latencies {
					pathLatency += latency.Microseconds()
					//log.Printf("%s: [%d]: %0.2f ms", key, item.id, float64(latency.Microseconds())/1000)
				}
				log.Printf("%s: [%d]: %0.2f MiB/sec, %0.2f ms avg., %0.2f ms med.\n", key, item.id, float64(item.bytes)/MiB/item.time.Seconds(), float64(pathLatency)/float64(len(item.latencies))/1000, 0.0)
			}
		}
		log.Printf("%s: %0.2f MiB/sec\n", key, pathThroughput[key]/MiB)
	}

	// log.Printf("Write iterations: %d\n", ratio_count)

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
