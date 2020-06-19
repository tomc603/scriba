package main

import (
	"flag"
	"fmt"
	"log"
	"math"
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

type ByDuration []time.Duration

var (
	VersionMajor string = "0"
	VersionMinor string = "7"
	VersionBuild string
	Debug        bool
	Verbose      bool
	//ratio_count  uint64
)

func (d ByDuration) Len() int           { return len(d) }
func (d ByDuration) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d ByDuration) Less(i, j int) bool { return d[i] < d[j] }

func (t *throughput) Average() time.Duration {
	var totalTime time.Duration

	for _, value := range t.latencies {
		totalTime += value
	}

	return time.Duration(int64(totalTime) / int64(len(t.latencies)))
}

func (t *throughput) Max() time.Duration {
	var maxTime time.Duration

	if len(t.latencies) == 0 {
		return maxTime
	}

	for _, value := range t.latencies {
		if value > maxTime {
			maxTime = value
		}
	}

	return maxTime
}

func (t *throughput) Min() time.Duration {
	var minTime time.Duration

	if len(t.latencies) == 0 {
		return minTime
	}

	minTime = time.Duration(math.MaxInt64)
	for _, value := range t.latencies {
		if value < minTime {
			minTime = value
		}
	}

	return minTime
}

func (t *throughput) Percentile(q float64) time.Duration {
	tempSlice := make([]time.Duration, len(t.latencies))

	copy(tempSlice, t.latencies)
	sort.Sort(ByDuration(tempSlice))
	k := float64(len(tempSlice)-1) * q
	floor := math.Floor(k)
	ceiling := math.Ceil(k)
	if floor == ceiling {
		return tempSlice[int(k)]
	}

	return (tempSlice[int(floor)] + tempSlice[int(ceiling)]) / 2
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
			}
			writerConfigs = append(writerConfigs, &wc)
			stats.Add(DevFromPath(pathValue))
			wg.Add(1)
			go writer(&wc, &wg)
			writerID++
		}
	}
	wg.Wait()

	pathThroughput := make(map[string]float64)
	for key, value := range results.d {
		for _, item := range value {
			pathThroughput[key] += float64(item.bytes) / item.time.Seconds()
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
