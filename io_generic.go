//go:build (!linux)

package main

import "syscall"

func readerFlags(direct bool) int {
	return syscall.O_RDONLY
}

func writerFlags(direct bool) int {
	return syscall.O_WRONLY
}
