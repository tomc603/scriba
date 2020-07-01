// +build !linux

package main

import (
	"log"
	"os"
)

func Allocate(path string, size int64) error {
	f, openErr := os.Create(path)
	if openErr != nil {
		log.Printf("ERROR: Unable to create %s. %s", path, openErr)
		return openErr
	}

	if allocErr := f.Truncate(size); allocErr != nil {
		log.Printf("Allocate(): ERROR: Unable to Truncate(%d) %s. %s", size, path, allocErr)
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
