package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
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
	stats  []*sysfsDiskStats
	device string
}

// TODO: Add stats for CPU and IRQ
type statsCollection struct {
	diskstats []*diskStats
	semaphore chan bool
	t         *time.Ticker
}

func (s *sysfsDiskStats) csv() string {
	var output string

	output += fmt.Sprintf("%d,", s.t.Unix())

	output += fmt.Sprintf("%d,", s.readIO)
	output += fmt.Sprintf("%d,", s.readMerges)
	output += fmt.Sprintf("%d,", s.readSectors)
	output += fmt.Sprintf("%d,", s.readTime)

	output += fmt.Sprintf("%d,", s.writeIO)
	output += fmt.Sprintf("%d,", s.writeMerges)
	output += fmt.Sprintf("%d,", s.writeSectors)
	output += fmt.Sprintf("%d,", s.writeTime)

	output += fmt.Sprintf("%d,", s.inFlight)
	output += fmt.Sprintf("%d,", s.ioTime)
	output += fmt.Sprintf("%d", s.timeInQueue)

	return fmt.Sprint(output)
}

func (s *sysfsDiskStats) String() string {
	var output string

	output += fmt.Sprintf("    %-15s:%15s\n", "Timestamp", s.t)

	output += fmt.Sprintf("    %-15s:%15d\n", "Read IO", s.readIO)
	output += fmt.Sprintf("    %-15s:%15d\n", "Read Merges", s.readMerges)
	output += fmt.Sprintf("    %-15s:%15d\n", "Read Sectors", s.readSectors)
	output += fmt.Sprintf("    %-15s:%15d\n", "Read Time", s.readTime)

	output += fmt.Sprintf("    %-15s:%15d\n", "Write IO", s.writeIO)
	output += fmt.Sprintf("    %-15s:%15d\n", "Write Merges", s.writeMerges)
	output += fmt.Sprintf("    %-15s:%15d\n", "Write Sectors", s.writeSectors)
	output += fmt.Sprintf("    %-15s:%15d\n", "Write Time", s.writeTime)

	output += fmt.Sprintf("    %-15s:%15d\n", "In Flight", s.inFlight)
	output += fmt.Sprintf("    %-15s:%15d\n", "IO Time", s.ioTime)
	output += fmt.Sprintf("    %-15s:%15d\n", "Time in Queue", s.timeInQueue)

	return fmt.Sprint(output)
}

func (s *statsCollection) Add(device string) {
	d := diskStats{device: device}

	for _, item := range s.diskstats {
		if item.device == device {
			if Debug {
				log.Printf("Device %s already being polled for stats\n", device)
			}
			return
		}
	}

	s.diskstats = append(s.diskstats, &d)
}

func (s *statsCollection) CollectStats() {
	s.t = time.NewTicker(1 * time.Second)
	for {
		select {
		case <-s.semaphore:
			s.t.Stop()
			return
		case <-s.t.C:
			for _, item := range s.diskstats {
				if Debug {
					log.Printf("Updating stats for %s\n", item.device)
				}

				if err := item.UpdateStats(); err != nil {
					log.Printf("Error updating stats for %s. %s\n", item.device, err)
				}
			}
		}
	}
}

func (s *statsCollection) csv() string {
	var output string

	output += "\"device\",\"timestamp\",\"read IO\",\"read merges\",\"read sectors\",\"read time\",\"write IO\",\"write merges\",\"write sectors\",\"write time\",\"inflight\",\"IO time\",\"time in queue\"\n"
	for _, item := range s.diskstats {
		output += fmt.Sprintf("%s\n", item.csv())
	}
	return output
}

func (s *statsCollection) String() string {
	var output string

	for _, item := range s.diskstats {
		output += item.csv()
	}
	return output
}

func (s *diskStats) csv() string {
	var output string

	for _, item := range s.stats {
		output += fmt.Sprintf("\"%s\",%s\n", s.device, item.csv())
	}
	return fmt.Sprint(output)
}

func (s *diskStats) String() string {
	var output string

	output += fmt.Sprintf("\n%s\n", s.device)
	for _, item := range s.stats {
		output += fmt.Sprintf("%s\n", item.String())
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
	s.stats = append(s.stats, &stat)

	return nil
}

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
	defer mountsFile.Close()

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
