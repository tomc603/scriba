//go:build !linux

package main

import (
	"errors"
	"log"
	"os"
)

func Allocate(path string, size int64, keep bool) error {
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
		if s.Size() == size && keep {
			if Debug {
				log.Printf("Allocate(): Requested size and existing file size are the same. Skipping.\n")
			}
			return nil
		}
	}

	truncErr := os.Truncate(path, size)
	if truncErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to Truncate(%d) %s. %s", size, path, truncErr)
		}
	}
	return truncErr
}
