package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	KiB = 1 << 10
	MiB = 1 << 20
	GiB = 1 << 30
	TiB = 1 << 40

	KB = 1e3
	MB = 1e6
	GB = 1e9
	TB = 1e12
)

// DevFromPath - Find the longest mountpoint prefixing path and return its matching device
func DevFromPath(path string) string {
	var candidate string
	var device string

	if Debug {
		log.Printf("Discovering device for path %s\n", path)
	}
	mountsFile, err := os.Open("/proc/self/mounts")
	if err != nil {
		if Debug {
			log.Printf("Unable to read mounts file. %s\n", err)
		}
		return ""
	}

	scanner := bufio.NewScanner(mountsFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		mountInfo := strings.Fields(scanner.Text())
		if strings.HasPrefix(path, mountInfo[1]) {
			if Debug {
				log.Printf("Matched mountpoint %s, dev %s\n", mountInfo[1], mountInfo[0])
			}

			if len(mountInfo[1]) > len(candidate) {
				if Debug {
					log.Printf("Updating candidate to %s from %s\n", mountInfo[1], candidate)
				}

				candidate = mountInfo[1]
				device = mountInfo[0]
			}
		}
	}
	if closeErr := mountsFile.Close(); closeErr != nil {
		log.Printf("WARNING: Unable to close /proc/self/mounts. %s\n", closeErr)
	}

	device = strings.TrimPrefix(device, "/dev/")
	if strings.HasPrefix(device, "sd") {
		device = strings.TrimRight(device, "123456789")
	}
	if strings.HasPrefix(device, "nvme") {
		// Strip the partition number off an NVMe device if a partition number exists.
		index := strings.LastIndex(device, "p")
		if index > 4 {
			// Since the first character can't be "p", we check for a higher index
			device = device[:index]
		}
	}

	return device
}

func humanizeSize(f float64, base2 bool) string {

	if base2 {
		switch {
		case f > TiB:
			return fmt.Sprintf("%0.2f TB", f/TiB)
		case f > GiB:
			return fmt.Sprintf("%0.2f GB", f/GiB)
		case f > MiB:
			return fmt.Sprintf("%0.2f MB", f/MiB)
		case f > KiB:
			return fmt.Sprintf("%0.2f KB", f/KiB)
		}
	} else {
		switch {
		case f > TB:
			return fmt.Sprintf("%0.2f TB", f/TB)
		case f > GB:
			return fmt.Sprintf("%0.2f GB", f/GB)
		case f > MB:
			return fmt.Sprintf("%0.2f MB", f/MB)
		case f > KB:
			return fmt.Sprintf("%0.2f KB", f/KB)
		}
	}

	// Whether we want base 10 or base 2, bytes are bytes.
	return fmt.Sprintf("%0.0f bytes", f)
}
