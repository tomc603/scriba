package main

import (
	"syscall"
)

func readerFlags(direct bool) int {
	if direct {
		return syscall.O_RDONLY | syscall.O_DIRECT
	}
	return syscall.O_RDONLY
}

func writerFlags(direct bool) int {
	if direct {
		return syscall.O_WRONLY | syscall.O_DIRECT
	}
	return syscall.O_WRONLY
}
