package stats

import (
	"fmt"
	"time"
)

// Capture statistics of Chord operations
type ChordStats interface {

	// How many jumps did a lookup take
	LookupNumberOfJumps(n int)

	// How long did a lookup take
	LookupTime(duration time.Duration)

	// Track successful cache results
	SuccessfulCacheResult()

	// Track how many lookups are performed
	LookupCountIncr()
}

// Drop all statistics
type BlackholeStats struct{}

func (t *BlackholeStats) LookupNumberOfJumps(n int) {}

func (t *BlackholeStats) LookupTime(duration time.Duration) {}

func (t *BlackholeStats) SuccessfulCacheResult() {}

func (t *BlackholeStats) LookupCountIncr() {}

var _ ChordStats = ChordStats(&BlackholeStats{})

// Just print the stats to the console
type PrintStats struct {
	LookupNumberOfJumpsArr []int
	LookupTimeArr          []time.Duration
	SuccessfulCacheResults int
	LookupCount            int
}

func NewPrintStats() *PrintStats {
	return &PrintStats{
		LookupNumberOfJumpsArr: make([]int, 0),
		LookupTimeArr:          make([]time.Duration, 0),
		SuccessfulCacheResults: 0,
		LookupCount:            0,
	}
}

func (t *PrintStats) LookupNumberOfJumps(n int) {
	t.LookupNumberOfJumpsArr = append(t.LookupNumberOfJumpsArr, n)
}

func (t *PrintStats) LookupTime(duration time.Duration) {
	t.LookupTimeArr = append(t.LookupTimeArr, duration)
}

func (t *PrintStats) SuccessfulCacheResult() {
	t.SuccessfulCacheResults++
}

func (t *PrintStats) LookupCountIncr() {
	t.LookupCount++
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
	fmt.Printf("\nCache hits: %v", t.SuccessfulCacheResults)
	fmt.Printf("\nLookups: %v", t.LookupCount)

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
