package main

import (
	"errors"
	"log"
	"os"
	"syscall"
)

const (
	FALLOC_FL_ZERO_RANGE = 0x10
)

func Allocate(path string, size int64) error {
	s, statErr := os.Stat(path)
	if statErr != nil {
		if !errors.Is(statErr, os.ErrNotExist) {
			log.Printf("Unable to access existing file %s. %s\n", path, statErr)
			return statErr
		}
	}

	// The file exists, so let's see if we can optimize away the fallocate.
	if s != nil {
		// If the requested size is the same as the existing file size, let's leave it alone for optimization's sake.
		if s.Size() == size {
			if Debug {
				log.Printf("Allocate(): Requested size and existing file size are the same. Skipping.\n")
			}
			return nil
		}

		// The target size is smaller than the existing file size. Let's truncate it to the requested size.
		if s.Size() > size {
			truncErr := os.Truncate(path, size)
			if truncErr != nil {
				if Debug {
					log.Printf("Allocate(): ERROR: Unable to truncate %s. %s\n", path, truncErr)
				}
			}
			return truncErr
		}
	}

	// Open the file for read/write, and create it if necessary
	f, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	defer f.Close()
	if openErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to OpenFile(%s, %d, %d). %s\n", path, os.O_RDWR|os.O_CREATE, 0666, openErr)
		}
		return openErr
	}

	// fallocate(2) the file to the requested size. This will zero the entire file's size and allocate
	// all metadata to guarantee space is available.
	allocErr := syscall.Fallocate(int(f.Fd()), FALLOC_FL_ZERO_RANGE, 0, size)
	if allocErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to fallocate(%d, 0, %d) %s. %s\n", f.Fd(), size, path, allocErr)
		}
	}
	return allocErr
}
