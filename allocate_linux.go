package main

import (
	"log"
	"os"
	"syscall"
)

const (
	FALLOC_FL_ZERO_RANGE = 0x10
)

func Allocate(path string, size int64) error {
	// Open the file for read/write, and create it if necessary
	f, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if openErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to OpenFile(%s, %d, %d). %s\n", path, os.O_RDWR|os.O_CREATE, 0666, openErr)
		}
		return openErr
	}

	// fallocate(2) the file to the requested size. This will zero the entire file's size and allocate
	// all metadata to guarantee space is available.
	if allocErr := syscall.Fallocate(int(f.Fd()), FALLOC_FL_ZERO_RANGE, 0, size); allocErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to fallocate(%d, 0, %d) %s. %s\n", f.Fd(), size, path, allocErr)
		}
		return allocErr
	}

	if closeErr := f.Close(); closeErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to Close() %s. %s\n", path, closeErr)
		}
		return closeErr
	}

	return nil
}
