package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"strconv"
	"time"
)

type sysfsDiskStats struct {
	t              time.Time
	readIO         int // Completed read IO requests
	readMerges     int
	readSectors    int // 512 byte reads
	readTime       int // Product of requests waiting and milliseconds that requests have waited
	writeIO        int // Completed write IO requests
	writeMerges    int
	writeSectors   int // 512 byte writes
	writeTime      int // Product of requests waiting and milliseconds that requests have waited
	inFlight       int // The number of IO requests that have been issued but not completed
	ioTime         int // The amount of time (ms) during which IO has been queued
	timeInQueue    int // The product of queued IO request time and queued requests
	discardIO      int // Completed discard IO requests
	discardMerges  int
	discardSectors int // 512 byte discards
	discardTime    int // Product of requests waiting and milliseconds that requests have waited
}

type diskStats struct {
	stats []sysfsDiskStats
	device string
}

func (s *diskStats) String() string {
	var output string

	for _, stat := range s.stats {
		output += fmt.Sprintf("\n%s\n", s.device)

		output += fmt.Sprintf("    %-15s:%15s\n","Timestamp", stat.t)

		output += fmt.Sprintf("    %-15s:%15d\n","Read IO", stat.readIO)
		output += fmt.Sprintf("    %-15s:%15d\n","Read Merges", stat.readMerges)
		output += fmt.Sprintf("    %-15s:%15d\n","Read Sectors", stat.readSectors)
		output += fmt.Sprintf("    %-15s:%15d\n","Read Time", stat.readTime)

		output += fmt.Sprintf("    %-15s:%15d\n","Write IO", stat.writeIO)
		output += fmt.Sprintf("    %-15s:%15d\n","Write Merges", stat.writeMerges)
		output += fmt.Sprintf("    %-15s:%15d\n","Write Sectors", stat.writeSectors)
		output += fmt.Sprintf("    %-15s:%15d\n","Write Time", stat.writeTime)

		output += fmt.Sprintf("    %-15s:%15d\n","In Flight", stat.inFlight)
		output += fmt.Sprintf("    %-15s:%15d\n","IO Time", stat.ioTime)
		output += fmt.Sprintf("    %-15s:%15d\n","Time in Queue", stat.timeInQueue)
	}
	return fmt.Sprint(output)
}

func (s *diskStats) UpdateStats() error {
	var statsFileData []byte
	var stat sysfsDiskStats
	var err error

	statsFileData, err = ioutil.ReadFile(fmt.Sprintf("/sys/block/%s/stat", s.device))
	if err != nil {
		return err
	}
	stats := strings.Fields(string(statsFileData))

	if stat.readIO, err = strconv.Atoi(stats[0]); err != nil {
		return err
	}
	if stat.readMerges, err = strconv.Atoi(stats[1]); err != nil {
		return err
	}
	if stat.readSectors, err = strconv.Atoi(stats[2]); err != nil {
		return err
	}
	if stat.readTime, err = strconv.Atoi(stats[3]); err != nil {
		return err
	}

	if stat.writeIO, err = strconv.Atoi(stats[4]); err != nil {
		return err
	}
	if stat.writeMerges, err = strconv.Atoi(stats[5]); err != nil {
		return err
	}
	if stat.writeSectors, err = strconv.Atoi(stats[6]); err != nil {
		return err
	}
	if stat.writeTime, err = strconv.Atoi(stats[7]); err != nil {
		return err
	}

	if stat.inFlight, err = strconv.Atoi(stats[8]); err != nil {
		return err
	}
	if stat.ioTime, err = strconv.Atoi(stats[9]); err != nil {
		return err
	}
	if stat.timeInQueue, err = strconv.Atoi(stats[10]); err != nil {
		return err
	}

	if len(stats) > 11 {
		if stat.discardIO, err = strconv.Atoi(stats[11]); err != nil {
			return err
		}
		if stat.discardMerges, err = strconv.Atoi(stats[12]); err != nil {
			return err
		}
		if stat.discardSectors, err = strconv.Atoi(stats[13]); err != nil {
			return err
		}
		if stat.discardTime, err = strconv.Atoi(stats[14]); err != nil {
			return err
		}
	}
	stat.t = time.Now()
	s.stats = append(s.stats, stat)

	return nil
}

func NewStatsCollector(path string) *diskStats {
	devicePath := DevFromPath(path)
	if devicePath == "" {
		return nil
	}

	return &diskStats{device: devicePath}
}

// DevFromPath - Find the longest mountpoint prefixing path and return its matching device
func DevFromPath(path string) string {
	var candidate string
	var device string

	if debug {
		log.Printf("Discovering device for path %s\n", path)
	}
	mountsFile, err := os.Open("/proc/self/mounts")
	if err != nil {
		if debug {
			log.Printf("Unable to read mounts file. %s\n", err)
		}
		return ""
	}
	defer mountsFile.Close()

	scanner := bufio.NewScanner(mountsFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		mountInfo := strings.Fields(scanner.Text())
		if strings.HasPrefix(path, mountInfo[1]) {
			if debug {
				log.Printf("Matched mountpoint %s, dev %s\n", mountInfo[1], mountInfo[0])
			}

			if len(mountInfo[1]) > len(candidate) {
				if debug {
					log.Printf("Updating candidate to %s from %s\n", mountInfo[1], candidate)
				}

				candidate = mountInfo[1]
				device = mountInfo[0]
			}
		}
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
