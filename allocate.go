// +build !linux

package main

import (
	"log"
	"os"
)

func Allocate(path string, size int64) error {
	truncErr := os.Truncate(path, size)
	if truncErr != nil {
		if Debug {
			log.Printf("Allocate(): ERROR: Unable to Truncate(%d) %s. %s", size, path, truncErr)
		}
	}
	return truncErr
}
