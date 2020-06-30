package main

import (
	"log"
	"os"
	"syscall"
)

func Allocate(path string, size int64) error {
	// Open the file for read/write, and create it if necessary
	f, open_err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if open_err != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to OpenFile(%s, %d, %d). %s\n", path, os.O_RDWR|os.O_CREATE, 0666, open_err)
		}
		return open_err
	}

	// fallocate(2) the file to the requested size. This will zero the entire file's size and allocate
	// all metadata to guarantee space is available.
	if alloc_err := syscall.Fallocate(int(f.Fd()), FALLOC_FL_ZERO_RANGE, 0, size); alloc_err != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to fallocate(%d, 0, %d) %s. %s\n", f.Fd(), size, path, alloc_err)
		}
		return alloc_err
	}

	if close_err := f.Close(); close_err != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to Close() %s. %s\n", path, close_err)
		}
		return close_err
	}

	return nil
}
