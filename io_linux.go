package main

import (
	"log"
	"syscall"
)

func readerFlags(direct bool) int {
	if direct {
		if Debug {
			log.Printf("readerFlags() Setting direct IO: %v\n", direct)
		}
		return syscall.O_RDONLY | syscall.O_DIRECT
	}
	return syscall.O_RDONLY
}

func writerFlags(direct bool) int {
	if direct {
		if Debug {
			log.Printf("writerFlags() Setting direct IO: %v\n", direct)
		}
		return syscall.O_WRONLY | syscall.O_DIRECT
	}
	return syscall.O_WRONLY
}
