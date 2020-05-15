package main

import (
	"crypto/rand"
	"flag"
	"io/ioutil"
	"log"
	"sync"
)

var debug bool

func writer(path string, outputSize int, flushSize int, wg *sync.WaitGroup) {
	bufSize := 32 * 1024 * 1024
	data := make([]byte, bufSize)
	writeTotal := 0

	defer wg.Done()

	if debug {
		log.Println("Generating random data buffer")
	}
	_, err := rand.Read(data)
	if err != nil {
		log.Printf("Error: %s\n", err)
		return
	}
	if debug {
		log.Printf("Generated %d random bytes", len(data))
	}

	tmpfile, err := ioutil.TempFile(path, "output*.data")
	if err != nil {
		log.Printf("Error: %s\n", err)
		return
	}
	//defer os.Remove(tmpfile.Name())

	for i := 0; i < outputSize/bufSize; i++ {
		if flushSize > 0 && flushSize < bufSize {
			for j := 0; j < bufSize/flushSize; j++ {
				n, err := tmpfile.Write(data[j*flushSize:flushSize])
				if err != nil {
					tmpfile.Close()
					log.Printf("Error: %s\n", err)
					return
				}
				writeTotal += n
			}
		} else {
			n, err := tmpfile.Write(data)
			if err != nil {
				tmpfile.Close()
				log.Printf("Error: %s\n", err)
				return
			}
			writeTotal += n
		}
	}
	if writeTotal < outputSize {
		n, err := tmpfile.Write(data[:outputSize-writeTotal])
		if err != nil {
			tmpfile.Close()
			log.Printf("Error: %s\n", err)
			writeTotal += n
			return
		}
	}
	tmpfile.Close()

	log.Printf("Wrote %d total bytes\n", writeTotal)
}

func main() {
	var size int
	var writers int
	var wg sync.WaitGroup

	flag.BoolVar(&debug, "debug", false, "Output debugging messages")
	flag.IntVar(&writers, "writers", 1, "Number of writer routines")
	flag.IntVar(&size, "size", 32*1024*1024, "File size for each writer")
	flag.Parse()

	log.Printf("Writers: %d\n", writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go writer("/tmp", size, 0, &wg)
	}

	wg.Wait()
}
