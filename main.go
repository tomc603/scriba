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

var debug bool

func writer(id int, path string, outputSize int, flushSize int, keep bool, wg *sync.WaitGroup) {
	bufSize := 32 * 1024 * 1024
	data := make([]byte, flushSize)
	writeTotal := 0

	defer wg.Done()

	if debug {
		log.Printf("[Writer %d] Generating random data buffer\n", id)
	}
	dr := NewDataReader(bufSize)
	if debug {
		log.Printf("[Writer %d] Generated %d random bytes", id, bufSize)
	}

	tmpfile, err := ioutil.TempFile(path, "output*.data")
	if err != nil {
		log.Printf("[Writer %d] Error: %s\n", id, err)
		return
	}
	if keep {
		defer os.Remove(tmpfile.Name())
	}

	log.Printf("[Writer %d] Starting writer\n", id)
	startTime := time.Now()
	for i := 0; i < outputSize / flushSize; i++ {
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
		if debug {
			log.Printf("[Writer %d] Wrote %d bytes - %d\n", id, n, writeTotal)
		}

		_ = tmpfile.Sync()
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
		if debug {
			log.Printf("[Writer %d] Wrote %d bytes - %d\n", id, n, writeTotal)
		}
	}
	_ = tmpfile.Sync()
	_ = tmpfile.Close()

	log.Printf("[Writer %d] Wrote %d bytes to %s (%d MB/s, %0.2f sec.)\n", id, writeTotal, tmpfile.Name(), (writeTotal/1048576)/int(time.Now().Sub(startTime).Seconds()), time.Now().Sub(startTime).Seconds())
}

func main() {
	var keep bool
	var size int
	var writers int
	var flushSize int
	var wg sync.WaitGroup

	flag.BoolVar(&debug, "debug", false, "Output debugging messages")
	flag.BoolVar(&keep, "keep", false, "Do not remove data files upon completion")
	//flag.BoolVar(&fallocate, "fallocate", false, "Use fallocate to pre-allocate files")
	flag.IntVar(&flushSize, "flush", 0, "Data each writer should write before calling Sync")
	flag.IntVar(&writers, "writers", 1, "Number of writer routines")
	flag.IntVar(&size, "size", 32*1024*1024, "File size for each writer")
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
	}

	writerID := 0
	for i := 0; i < writers; i++ {
		for _, pathValue := range flag.Args() {
			wg.Add(1)
			go writer(writerID, pathValue, size, flushSize, keep, &wg)
			writerID++
		}
	}

	wg.Wait()
}
