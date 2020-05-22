package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

var (
	debug bool
)

func sizeHumanizer(i int, base2 bool) string {
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

	size := float64(i)
	if base2 {
		switch {
		case size > base2tera:
			return fmt.Sprintf("%0.2f TB", size/base2tera)
		case size > base2giga:
			return fmt.Sprintf("%0.2f GB", size/base2giga)
		case size > base2mega:
			return fmt.Sprintf("%0.2f MB", size/base2mega)
		case size > base2kilo:
			return fmt.Sprintf("%0.2f KB", size/base2kilo)
		}
	} else {
		switch {
		case size > base10tera:
			return fmt.Sprintf("%0.2f TB", size/base10tera)
		case size > base10giga:
			return fmt.Sprintf("%0.2f GB", size/base10giga)
		case size > base10mega:
			return fmt.Sprintf("%0.2f MB", size/base10mega)
		case size > base10kilo:
			return fmt.Sprintf("%0.2f KB", size/base10kilo)
		}
	}

	// Whether we want base 10 or base 2, bytes are bytes.
	return fmt.Sprintf("%d bytes", i)
}

func writer(id int, path string, outputSize int, flushSize int, keep bool, recordStats bool, wg *sync.WaitGroup) {
	readerBufSize := 32 * 1024 * 1024
	data := make([]byte, flushSize)
	stats := NewStatsCollector(path)
	writeTotal := 0

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
	lastStatTime := time.Now()
	if recordStats && stats != nil {
		if err := stats.UpdateStats(); err != nil {
			log.Printf("[Writer %d] Error gathering stats. %s\n", id, err)
		}
		lastStatTime = time.Now()
	}
	for i := 0; i < outputSize/flushSize; i++ {
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
		_ = tmpfile.Sync()

		if time.Now().Sub(lastStatTime).Seconds() > 1.0 && stats != nil {
			if err := stats.UpdateStats(); err != nil {
				log.Printf("[Writer %d] Error gathering stats. %s\n", id, err)
			}
			lastStatTime = time.Now()
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
	if recordStats && stats != nil {
		stats.UpdateStats()
	}

	log.Printf(
		"[Writer %d] Wrote %s to %s (%s/s, %0.2f sec.)\n",
		id,
		sizeHumanizer(writeTotal, true),
		tmpfile.Name(),
		sizeHumanizer(int(float64(writeTotal)/time.Now().Sub(startTime).Seconds()), true),
		time.Now().Sub(startTime).Seconds(),
	)
	if recordStats {
		fmt.Printf("\n** STATS **\n%s\n", stats)
	}
}

func main() {
	var keep bool
	var recordStats bool
	var size int
	var writers int
	var flushSize int
	var wg sync.WaitGroup

	flag.BoolVar(&debug, "debug", false, "Output debugging messages")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	flag.BoolVar(&recordStats, "stats", false, "Track block device IO statistics while testing")
	//flag.BoolVar(&fallocate, "fallocate", false, "Use fallocate to pre-allocate files")
	flag.IntVar(&flushSize, "flush", 65536, "The amount of ata each writer should write before calling Sync")
	flag.IntVar(&writers, "writers", 1, "The number of writer routines")
	flag.IntVar(&size, "size", 32*1024*1024, "The target file size for each writer")
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

	// TODO: Sanity check the flushSize value

	writerID := 0
	log.Printf("Starting %d writers\n", writers)
	for i := 0; i < writers; i++ {
		for _, pathValue := range flag.Args() {
			wg.Add(1)
			go writer(writerID, pathValue, size, flushSize, keep, recordStats, &wg)
			writerID++
		}
	}

	wg.Wait()
}
