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

type diskStats struct {
	Device string
	Stats  []*sysfsDiskStats
}

type IOStats struct {
	sync.Mutex
	ReadThroughput  map[string][]*Throughput
	WriteThroughput map[string][]*Throughput
}

type sysfsDiskStats struct {
	DiscardIO      int // Completed discard IO requests
	DiscardMerges  int
	DiscardSectors int // 512 byte discards
	DiscardTime    int // Product of requests waiting and milliseconds that requests have waited
	InFlight       int // The number of IO requests that have been issued but not completed
	IOTime         int // The amount of time (ms) during which IO has been queued
	TimeInQueue    int // The product of queued IO request time and queued requests
	ReadIO         int // Completed read IO requests
	ReadMerges     int
	ReadSectors    int // 512 byte reads
	ReadTime       int // Product of requests waiting and milliseconds that requests have waited
	Time           time.Time
	WriteIO        int // Completed write IO requests
	WriteMerges    int
	WriteSectors   int // 512 byte writes
	WriteTime      int // Product of requests waiting and milliseconds that requests have waited
}

// TODO: Add stats for CPU, memory, and interrupts
type SysStatsCollection struct {
	//CPU     []*cpuStats
	//Memory  []*memoryStats
	Disk      []*diskStats
	Semaphore chan bool
	t         *time.Ticker
}

type Throughput struct {
	ID              int
	Latencies       []time.Duration
	sortedLatencies []time.Duration
}

type byDuration []time.Duration
type byThroughputID []*Throughput

func (d byDuration) Len() int           { return len(d) }
func (d byDuration) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d byDuration) Less(i, j int) bool { return d[i] < d[j] }

func (v byThroughputID) Len() int           { return len(v) }
func (v byThroughputID) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byThroughputID) Less(i, j int) bool { return v[i].ID < v[j].ID }

func (s *diskStats) Csv() string {
	var output string

	for _, item := range s.Stats {
		output += fmt.Sprintf("\"%s\",%s\n", s.Device, item.Csv())
	}
	return fmt.Sprint(output)
}

func (s *diskStats) String() string {
	var output string

	output += fmt.Sprintf("\n%s\n", s.Device)
	for _, item := range s.Stats {
		output += fmt.Sprintf("%s\n", item.String())
	}
	return fmt.Sprint(output)
}

func (s *diskStats) UpdateStats() error {
	var statsFileData []byte
	var stat sysfsDiskStats
	var err error

	statsFileData, err = ioutil.ReadFile(fmt.Sprintf("/sys/block/%s/stat", s.Device))
	if err != nil {
		return err
	}
	stats := strings.Fields(string(statsFileData))

	if stat.ReadIO, err = strconv.Atoi(stats[0]); err != nil {
		return err
	}
	if stat.ReadMerges, err = strconv.Atoi(stats[1]); err != nil {
		return err
	}
	if stat.ReadSectors, err = strconv.Atoi(stats[2]); err != nil {
		return err
	}
	if stat.ReadTime, err = strconv.Atoi(stats[3]); err != nil {
		return err
	}

	if stat.WriteIO, err = strconv.Atoi(stats[4]); err != nil {
		return err
	}
	if stat.WriteMerges, err = strconv.Atoi(stats[5]); err != nil {
		return err
	}
	if stat.WriteSectors, err = strconv.Atoi(stats[6]); err != nil {
		return err
	}
	if stat.WriteTime, err = strconv.Atoi(stats[7]); err != nil {
		return err
	}

	if stat.InFlight, err = strconv.Atoi(stats[8]); err != nil {
		return err
	}
	if stat.IOTime, err = strconv.Atoi(stats[9]); err != nil {
		return err
	}
	if stat.TimeInQueue, err = strconv.Atoi(stats[10]); err != nil {
		return err
	}

	if len(stats) > 11 {
		if stat.DiscardIO, err = strconv.Atoi(stats[11]); err != nil {
			return err
		}
		if stat.DiscardMerges, err = strconv.Atoi(stats[12]); err != nil {
			return err
		}
		if stat.DiscardSectors, err = strconv.Atoi(stats[13]); err != nil {
			return err
		}
		if stat.DiscardTime, err = strconv.Atoi(stats[14]); err != nil {
			return err
		}
	}
	stat.Time = time.Now()
	s.Stats = append(s.Stats, &stat)

	return nil
}

func (s *IOStats) Write(dir string) error {
	//TODO: Output a timestamp or row count when writing latency data
	writeStatsFile, writeFileError := os.OpenFile(path.Join(dir, "writers.csv"), os.O_CREATE|os.O_RDWR, 0644)
	if writeFileError != nil {
		return writeFileError
	}

	if _, err := writeStatsFile.WriteString("\"path\",\"worker id\",\"latency us\"\n"); err != nil {
		log.Printf("ERROR: Unable to write to writer stats file. %s\n", err)
		return err
	}
	for key, value := range s.WriteThroughput {
		sort.Sort(byThroughputID(value))

		for _, item := range value {
			for _, latency := range item.Latencies {
				if _, err := writeStatsFile.WriteString(fmt.Sprintf("\"%s\",%d,%d\n", key, item.ID, latency.Microseconds())); err != nil {
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
	for key, value := range s.ReadThroughput {
		sort.Sort(byThroughputID(value))

		for _, item := range value {
			for _, latency := range item.Latencies {
				if _, err := readStatsFile.WriteString(fmt.Sprintf("\"%s\",%d,%d\n", key, item.ID, latency.Microseconds())); err != nil {
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

func (s *sysfsDiskStats) Csv() string {
	var output string

	output += fmt.Sprintf("%d,", s.Time.Unix())

	output += fmt.Sprintf("%d,", s.ReadIO)
	output += fmt.Sprintf("%d,", s.ReadMerges)
	output += fmt.Sprintf("%d,", s.ReadSectors)
	output += fmt.Sprintf("%d,", s.ReadTime)

	output += fmt.Sprintf("%d,", s.WriteIO)
	output += fmt.Sprintf("%d,", s.WriteMerges)
	output += fmt.Sprintf("%d,", s.WriteSectors)
	output += fmt.Sprintf("%d,", s.WriteTime)

	output += fmt.Sprintf("%d,", s.InFlight)
	output += fmt.Sprintf("%d,", s.IOTime)
	output += fmt.Sprintf("%d", s.TimeInQueue)

	return output
}

func (s *sysfsDiskStats) String() string {
	var output string

	output += fmt.Sprintf("    %-15s:%15s\n", "Timestamp", s.Time)

	output += fmt.Sprintf("    %-15s:%15d\n", "Read IO", s.ReadIO)
	output += fmt.Sprintf("    %-15s:%15d\n", "Read Merges", s.ReadMerges)
	output += fmt.Sprintf("    %-15s:%15d\n", "Read Sectors", s.ReadSectors)
	output += fmt.Sprintf("    %-15s:%15d\n", "Read Time", s.ReadTime)

	output += fmt.Sprintf("    %-15s:%15d\n", "Write IO", s.WriteIO)
	output += fmt.Sprintf("    %-15s:%15d\n", "Write Merges", s.WriteMerges)
	output += fmt.Sprintf("    %-15s:%15d\n", "Write Sectors", s.WriteSectors)
	output += fmt.Sprintf("    %-15s:%15d\n", "Write Time", s.WriteTime)

	output += fmt.Sprintf("    %-15s:%15d\n", "In Flight", s.InFlight)
	output += fmt.Sprintf("    %-15s:%15d\n", "IO Time", s.IOTime)
	output += fmt.Sprintf("    %-15s:%15d\n", "Time in Queue", s.TimeInQueue)

	return fmt.Sprint(output)
}

func (s *SysStatsCollection) Add(device string) {
	d := diskStats{Device: device}

	for _, item := range s.Disk {
		if item.Device == device {
			if Debug {
				log.Printf("Device %s already being polled for stats\n", device)
			}
			return
		}
	}

	s.Disk = append(s.Disk, &d)
}

func (s *SysStatsCollection) CollectStats() {
	s.t = time.NewTicker(1 * time.Second)
	for {
		select {
		case <-s.Semaphore:
			s.t.Stop()
			return
		case <-s.t.C:
			for _, item := range s.Disk {
				if Debug {
					log.Printf("Updating stats for %s\n", item.Device)
				}

				if err := item.UpdateStats(); err != nil {
					log.Printf("Error updating stats for %s. %s\n", item.Device, err)
				}
			}
		}
	}
}

func (s *SysStatsCollection) Csv() string {
	var output string

	output += "\"device\",\"timestamp\",\"read IO\",\"read merges\",\"read sectors\",\"read time\",\"write IO\",\"write merges\",\"write sectors\",\"write time\",\"inflight\",\"IO time\",\"time in queue\"\n"
	for _, item := range s.Disk {
		output += fmt.Sprintf("%s\n", item.Csv())
	}
	return output
}

func (s *SysStatsCollection) String() string {
	var output string

	for _, item := range s.Disk {
		output += item.Csv()
	}
	return output
}

func (s *SysStatsCollection) Write(dir string) error {
	for _, value := range s.Disk {
		diskStatsFile, diskFileError := os.OpenFile(path.Join(dir, fmt.Sprintf("diskstats.%s.csv", value.Device)), os.O_CREATE|os.O_RDWR, 0644)
		if diskFileError != nil {
			return diskFileError
		}

		if _, err := diskStatsFile.WriteString("device" +
			",\"time\",\"reads completed\",\"read merges\",\"read sectors\",\"read time\"" +
			",\"writes completed\",\"write merges\",\"write sectors\",\"write time\"" +
			",\"in flight\",\"io time\",\"time in queue\"\n"); err != nil {
			log.Printf("ERROR: Unable to write to writer stats file. %s\n", err)
			return err
		}

		for _, item := range value.Stats {
			if _, err := diskStatsFile.WriteString(fmt.Sprintf("%s,%s\n", value.Device, item.Csv())); err != nil {
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

func (t *Throughput) Average() time.Duration {
	var totalTime time.Duration

	for _, value := range t.Latencies {
		totalTime += value
	}

	return time.Duration(int64(totalTime) / int64(len(t.Latencies)))
}

func (t *Throughput) Max() time.Duration {
	if len(t.Latencies) == 0 {
		return 0
	}

	t.Sort()
	return t.sortedLatencies[len(t.sortedLatencies)-1]
}

func (t *Throughput) Min() time.Duration {
	if len(t.Latencies) == 0 {
		return 0
	}

	t.Sort()
	return t.sortedLatencies[0]
}

func (t *Throughput) Percentile(q float64) time.Duration {
	t.Sort()

	k := float64(len(t.sortedLatencies)-1) * q
	floor := math.Floor(k)
	ceiling := math.Ceil(k)
	if floor == ceiling {
		return t.sortedLatencies[int(k)]
	}

	return (t.sortedLatencies[int(floor)] + t.sortedLatencies[int(ceiling)]) / 2
}

func (t *Throughput) Sort() {
	if len(t.sortedLatencies) != len(t.Latencies) {
		t.sortedLatencies = make([]time.Duration, len(t.Latencies))
		copy(t.sortedLatencies, t.Latencies)
		sort.Sort(byDuration(t.sortedLatencies))
	}
}

func (t *Throughput) String() string {
	return fmt.Sprintf(
		"Min: %d us, Max: %d us, Avg: %d us, P50: %d us, P95: %d us, P99: %d us",
		t.Min().Microseconds(), t.Max().Microseconds(),
		t.Average().Microseconds(), t.Percentile(0.50).Microseconds(),
		t.Percentile(0.95).Microseconds(), t.Percentile(0.99).Microseconds(),
	)
}
