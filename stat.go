package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
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

// TODO: Add stats for CPU, memory, and interrupts
type sysStatsCollection struct {
	//cpu       []*cpuStats
	//memory    []*memoryStats
	disk      []*diskStats
	semaphore chan bool
	t         *time.Ticker
}

type throughput struct {
	id        int
	bytes     int64
	latencies []time.Duration
	time      time.Duration
}

type ioStats struct {
	sync.Mutex
	readThroughput  map[string][]*throughput
	writeThroughput map[string][]*throughput
}

type ByDuration []time.Duration

func (d ByDuration) Len() int           { return len(d) }
func (d ByDuration) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d ByDuration) Less(i, j int) bool { return d[i] < d[j] }

func (t *throughput) Average() time.Duration {
	var totalTime time.Duration

	for _, value := range t.latencies {
		totalTime += value
	}

	return time.Duration(int64(totalTime) / int64(len(t.latencies)))
}

func (t *throughput) Max() time.Duration {
	var maxTime time.Duration

	if len(t.latencies) == 0 {
		return maxTime
	}

	for _, value := range t.latencies {
		if value > maxTime {
			maxTime = value
		}
	}

	return maxTime
}

func (t *throughput) Min() time.Duration {
	var minTime time.Duration

	if len(t.latencies) == 0 {
		return minTime
	}

	minTime = time.Duration(math.MaxInt64)
	for _, value := range t.latencies {
		if value < minTime {
			minTime = value
		}
	}

	return minTime
}

func (t *throughput) Percentile(q float64) time.Duration {
	tempSlice := make([]time.Duration, len(t.latencies))

	copy(tempSlice, t.latencies)
	sort.Sort(ByDuration(tempSlice))
	k := float64(len(tempSlice)-1) * q
	floor := math.Floor(k)
	ceiling := math.Ceil(k)
	if floor == ceiling {
		return tempSlice[int(k)]
	}

	return (tempSlice[int(floor)] + tempSlice[int(ceiling)]) / 2
}

func (t *throughput) String() string {
	return fmt.Sprintf(
		"%0.2f MiB/sec, Min: %d us, Max: %d us, Avg: %d us, P50: %d us, P95: %d us, P99: %d us",
		float64(t.bytes)/MiB/t.time.Seconds(), t.Min().Microseconds(), t.Max().Microseconds(),
		t.Average().Microseconds(), t.Percentile(0.50).Microseconds(),
		t.Percentile(0.95).Microseconds(), t.Percentile(0.99).Microseconds(),
	)
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

	return output
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

func (s *sysStatsCollection) Add(device string) {
	d := diskStats{device: device}

	for _, item := range s.disk {
		if item.device == device {
			if Debug {
				log.Printf("Device %s already being polled for stats\n", device)
			}
			return
		}
	}

	s.disk = append(s.disk, &d)
}

func (s *sysStatsCollection) CollectStats() {
	s.t = time.NewTicker(1 * time.Second)
	for {
		select {
		case <-s.semaphore:
			s.t.Stop()
			return
		case <-s.t.C:
			for _, item := range s.disk {
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

func (s *sysStatsCollection) csv() string {
	var output string

	output += "\"device\",\"timestamp\",\"read IO\",\"read merges\",\"read sectors\",\"read time\",\"write IO\",\"write merges\",\"write sectors\",\"write time\",\"inflight\",\"IO time\",\"time in queue\"\n"
	for _, item := range s.disk {
		output += fmt.Sprintf("%s\n", item.csv())
	}
	return output
}

func (s *sysStatsCollection) String() string {
	var output string

	for _, item := range s.disk {
		output += item.csv()
	}
	return output
}

func (s *sysStatsCollection) Write(dir string) error {
	for _, value := range s.disk {
		diskStatsFile, diskFileError := os.OpenFile(path.Join(dir, fmt.Sprintf("diskstats.%s.csv", value.device)), os.O_CREATE|os.O_RDWR, 0644)
		if diskFileError != nil {
			return diskFileError
		}

		if _, err := diskStatsFile.WriteString("device" +
			",\"time\",\"reads completed\",\"read merges\",\"read sectors\",\"read time\"" +
			",\"writes completed\",\"write merges\",\"write sectors\",\"write time\"\n" +
			",\"in flight\",\"io time\",\"time in queue\""); err != nil {
			log.Printf("ERROR: Unable to write to writer stats file. %s\n", err)
			return err
		}

		for _, item := range value.stats {
			if _, err := diskStatsFile.WriteString(fmt.Sprintf("%s, %s\n", value.device, item.csv())); err != nil {
				log.Printf("ERROR: Unable to write to writer stats file. %s\n", err)
				return err
			}
		}

		_ = diskStatsFile.Sync()
		if closeErr := diskStatsFile.Close(); closeErr != nil {
			log.Printf("ERROR: Unable to close disk stats file. %s\n", closeErr)
			return closeErr
		}
	}

	return nil
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

func (s *ioStats) Write(dir string) error {
	writeStatsFile, writeFileError := os.OpenFile(path.Join(dir, "writers.csv"), os.O_CREATE|os.O_RDWR, 0644)
	if writeFileError != nil {
		return writeFileError
	}

	if _, err := writeStatsFile.WriteString("\"path\",\"worker id\",\"latency us\"\n"); err != nil {
		log.Printf("ERROR: Unable to write to writer stats file. %s\n", err)
		return err
	}
	for key, value := range s.writeThroughput {
		for _, item := range value {
			for _, latency := range item.latencies {
				if _, err := writeStatsFile.WriteString(fmt.Sprintf("\"%s\",%d,%d\n", key, item.id, latency.Microseconds())); err != nil {
					log.Printf("ERROR: Unable to write to writer stats file. %s\n", err)
					return err
				}
			}
		}
	}
	_ = writeStatsFile.Sync()
	if closeErr := writeStatsFile.Close(); closeErr != nil {
		log.Printf("ERROR: Unable to close writer stats file. %s\n", closeErr)
		return closeErr
	}

	readStatsFile, readFileError := os.OpenFile(path.Join(dir, "readers.csv"), os.O_CREATE|os.O_RDWR, 0644)
	if readFileError != nil {
		return readFileError
	}

	if _, err := readStatsFile.WriteString("\"path\",\"worker id\",\"latency us\"\n"); err != nil {
		log.Printf("ERROR: Unable to write to reader stats file. %s\n", err)
		return err
	}
	for key, value := range s.readThroughput {
		for _, item := range value {
			for _, latency := range item.latencies {
				if _, err := readStatsFile.WriteString(fmt.Sprintf("\"%s\",%d,%d\n", key, item.id, latency.Microseconds())); err != nil {
					log.Printf("ERROR: Unable to write to reader stats file. %s\n", err)
					return err
				}
			}
		}
	}
	_ = readStatsFile.Sync()
	if closeErr := readStatsFile.Close(); closeErr != nil {
		log.Printf("ERROR: Unable to close reader stats file. %s\n", closeErr)
		return closeErr
	}

	return nil
}
