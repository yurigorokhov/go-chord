package main

import (
	"errors"
	"go-chord"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Captures a delay and the probability of it occuring
type ProbabilityDelay struct {
	Probability float64
	Delay       uint64
}

// Create a new probablility configuration based on a string representation
// '200:.1|300:.2|0:.7' means delay 200ms 10% of the time, 300ms 20% of the time, and 0 the rest of the time"
func NewProbabilityDelaysFromStr(delayStr string) ([]ProbabilityDelay, error) {
	splitStr := strings.Split(delayStr, "|")
	if len(splitStr) == 0 {
		return []ProbabilityDelay{}, nil
	}
	result := make([]ProbabilityDelay, 0)
	totalProb := 0.0
	for _, str := range splitStr {
		splitStr2 := strings.SplitN(str, ":", 2)
		delay, err := strconv.ParseUint(splitStr2[0], 10, 64)
		if err != nil {
			return []ProbabilityDelay{}, err
		}
		prob, err := strconv.ParseFloat(splitStr2[1], 64)
		if err != nil {
			return []ProbabilityDelay{}, err
		}
		result = append(result, ProbabilityDelay{
			Probability: prob + totalProb,
			Delay:       delay,
		})
		totalProb += prob
	}
	if totalProb != 1.0 {
		return []ProbabilityDelay{}, errors.New("The probabilities do not add up to 1")
	}
	return result, nil
}

// Config for DelayedTCPTransport
// All delays are in milliseconds
type DelayTCPConfig struct {
	FindSuccessorsDelay uint64
	RandomDelays        []ProbabilityDelay
}

func (c *DelayTCPConfig) MaxPossibleDelay() uint64 {

	// find the maximum delay in RandomDelays
	var maxDelay uint64 = 0
	for _, d := range c.RandomDelays {
		if d.Delay > maxDelay {
			maxDelay = d.Delay
		}
	}
	return c.FindSuccessorsDelay + maxDelay
}

// Create a new delay config with default values
func DefaultDelayConfig() *DelayTCPConfig {
	return &DelayTCPConfig{
		FindSuccessorsDelay: 10,
		RandomDelays:        []ProbabilityDelay{},
	}
}

/*
DelayedTCPTransport provides a TCP transport layer with contrived network delays.
This is for simulation only!
*/
type DelayedTCPTransport struct {
	*chord.TCPTransport
	config     *DelayTCPConfig
	randSource *rand.Rand
}

// Creates TCPTransport wrapped with arbitrary delays
func InitDelayedTCPTransport(listen string, timeout time.Duration, config *DelayTCPConfig) (*DelayedTCPTransport, error) {
	tcpTransport, err := chord.InitTCPTransport(listen, timeout)
	if err != nil {
		return nil, err
	}
	return &DelayedTCPTransport{
		TCPTransport: tcpTransport,
		config:       config,
		randSource:   rand.New(rand.NewSource(time.Now().Unix())),
	}, nil
}

func (t *DelayedTCPTransport) FindSuccessors(vn *chord.Vnode, n int, k []byte, meta chord.LookupMetaData) (chord.LookupMetaData, []*chord.Vnode, error) {
	if t.config.FindSuccessorsDelay > 0 {
		time.Sleep(time.Duration(t.config.FindSuccessorsDelay * uint64(time.Millisecond)))
	}
	if len(t.config.RandomDelays) > 0 {

		// pick a random number in the range [0,1)
		r := t.randSource.Float64()

		// find the maximum probability that is smaller than rand, use the delay that corresponds to that probability
		j := -1
		for i, d := range t.config.RandomDelays {
			if d.Probability > r && (j == -1 || d.Probability < t.config.RandomDelays[j].Probability) {
				j = i
			}
		}
		if j > -1 {
			time.Sleep(time.Duration(t.config.RandomDelays[j].Delay * uint64(time.Millisecond)))
		}
	}
	return t.TCPTransport.FindSuccessors(vn, n, k, meta)
}

var _ = chord.Transport(&DelayedTCPTransport{})
