package main

import (
	"errors"
	"go-chord"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Create a new probablility configuration based on a string representation
// '200:.1|300:.2|0:.7' means delay 200ms 10% of the time, 300ms 20% of the time, and 0 the rest of the time"
func NewProbabilityDelaysFromStr(delayStr string) ([]chord.ProbabilityDelay, error) {
	splitStr := strings.Split(delayStr, "|")
	if len(splitStr) == 0 {
		return []chord.ProbabilityDelay{}, nil
	}
	result := make([]chord.ProbabilityDelay, 0)
	totalProb := 0.0
	for _, str := range splitStr {
		splitStr2 := strings.SplitN(str, ":", 2)
		delay, err := strconv.ParseUint(splitStr2[0], 10, 64)
		if err != nil {
			return []chord.ProbabilityDelay{}, err
		}
		prob, err := strconv.ParseFloat(splitStr2[1], 64)
		if err != nil {
			return []chord.ProbabilityDelay{}, err
		}
		result = append(result, chord.ProbabilityDelay{
			Probability: prob + totalProb,
			Delay:       delay,
		})
		totalProb += prob
	}
	if totalProb != 1.0 {
		return []chord.ProbabilityDelay{}, errors.New("The probabilities do not add up to 1")
	}
	return result, nil
}

// Create a new delay config with default values
func DefaultDelayConfig() *chord.DelayTCPConfig {
	return &chord.DelayTCPConfig{
		FindSuccessorsDelay: 10,
		RandomDelays:        []chord.ProbabilityDelay{},
	}
}

/*
DelayedTCPTransport provides a TCP transport layer with contrived network delays.
This is for simulation only!
*/
type DelayedTCPTransport struct {
	*chord.TCPTransport
	config     *chord.DelayTCPConfig
	randSource *rand.Rand
}

// Creates TCPTransport wrapped with arbitrary delays
func InitDelayedTCPTransport(listen string, timeout time.Duration, config *chord.DelayTCPConfig) (*DelayedTCPTransport, error) {
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
