package stats

import (
	"fmt"
	"time"

	"github.com/quipo/statsd"
)

// Capture statistics of Chord operations
type ChordStats interface {

	// How many jumps did a lookup take
	LookupNumberOfJumps(n int)

	// How long did a lookup take
	LookupTime(duration time.Duration)
}

// Drop all statistics
type BlackholeStats struct{}

func (t *BlackholeStats) LookupNumberOfJumps(n int) {}

func (t *BlackholeStats) LookupTime(duration time.Duration) {}

var _ ChordStats = ChordStats(&BlackholeStats{})

// Just print the stats to the console
type PrintStats struct {
	LookupNumberOfJumpsArr []int
	LookupTimeArr          []time.Duration
}

func NewPrintStats() *PrintStats {
	return &PrintStats{
		LookupNumberOfJumpsArr: make([]int, 0),
		LookupTimeArr:          make([]time.Duration, 0),
	}
}

func (t *PrintStats) LookupNumberOfJumps(n int) {
	t.LookupNumberOfJumpsArr = append(t.LookupNumberOfJumpsArr, n)
}

func (t *PrintStats) LookupTime(duration time.Duration) {
	t.LookupTimeArr = append(t.LookupTimeArr, duration)
}

func (t *PrintStats) Print() {
	numJumps := make([]float64, 0)
	for _, n := range t.LookupNumberOfJumpsArr {
		numJumps = append(numJumps, float64(n))
	}
	fmt.Printf("\n\nNumber of jumps: ")
	fmt.Printf("\nMin: %v", findMin(numJumps))
	fmt.Printf("\nMax: %v", findMax(numJumps))
	fmt.Printf("\nAvg: %v", findAvg(numJumps))

	lookupTime := make([]float64, 0)
	for _, n := range t.LookupTimeArr {
		lookupTime = append(lookupTime, n.Seconds()*1000)
	}
	fmt.Printf("\n\nLookup time (milliseconds): ")
	fmt.Printf("\nMin: %v", findMin(lookupTime))
	fmt.Printf("\nMax: %v", findMax(lookupTime))
	fmt.Printf("\nAvg: %v", findAvg(lookupTime))
}

func findMin(data []float64) float64 {
	min := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] < min {
			min = data[i]
		}
	}
	return min
}

func findMax(data []float64) float64 {
	max := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] > max {
			max = data[i]
		}
	}
	return max
}

func findAvg(data []float64) float64 {
	sum := 0.0
	for i := 0; i < len(data); i++ {
		sum += data[i]
	}
	return sum / float64(len(data))
}

var _ ChordStats = ChordStats(&PrintStats{})

// Send stats to statsd
type StatsdClient struct {
	Stats *statsd.StatsdBuffer
}

func NewStatsdClient(simulationPrefix string) *StatsdClient {
	statsdClient := statsd.NewStatsdClient("localhost:8125", simulationPrefix)
	statsdClient.CreateSocket()
	interval := time.Second * 2
	stats := statsd.NewStatsdBuffer(interval, statsdClient)
	return &StatsdClient{
		Stats: stats,
	}
}

func (t *StatsdClient) LookupNumberOfJumps(n int) {
	t.Stats.Total(".lookup.jumps.num", int64(n))
}

func (t *StatsdClient) LookupTime(duration time.Duration) {
	t.Stats.PrecisionTiming(".lookup.time", duration)
}

var _ ChordStats = ChordStats(&StatsdClient{})
