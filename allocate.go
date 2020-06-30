// +build !linux

package main

import (
	"log"
	"os"
)

func Allocate(path string, size int64) error {
	f, open_err := os.Create(path)
	if open_err != nil {
		log.Printf("ERROR: Unable to create %s. %s", path, open_err)
		return open_err
	}

	if alloc_err := f.Truncate(size); alloc_err != nil {
		log.Printf("Allocate(): ERROR: Unable to Truncate(%d) %s. %s", size, path, alloc_err)
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
