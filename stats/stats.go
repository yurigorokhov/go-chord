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
}

// Drop all statistics
type BlackholeStats struct{}

func (t *BlackholeStats) LookupNumberOfJumps(n int) {}

func (t *BlackholeStats) LookupTime(duration time.Duration) {}

var _ ChordStats = ChordStats(&BlackholeStats{})

// Just print the stats to the console
type PrintStats struct{}

func (t *PrintStats) LookupNumberOfJumps(n int) {
	fmt.Printf("\nSTATS: number jumps for lookup: %v", n)
}

func (t *PrintStats) LookupTime(duration time.Duration) {
	fmt.Printf("\nSTATS: lookup took %v", duration)
}

var _ ChordStats = ChordStats(&PrintStats{})
