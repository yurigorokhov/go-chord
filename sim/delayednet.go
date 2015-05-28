package main

import (
	"go-chord"
	"math/rand"
	"time"
)

// Config for DelayedTCPTransport
// All delays are in milliseconds
type DelayTCPConfig struct {
	FindSuccessorsDelay         uint64
	FindSuccessorRandomDelayMax uint64
}

func (c *DelayTCPConfig) MaxPossibleDelay() uint64 {
	return c.FindSuccessorsDelay + c.FindSuccessorRandomDelayMax
}

// Create a new delay config with default values
func DefaultDelayConfig() *DelayTCPConfig {
	return &DelayTCPConfig{
		FindSuccessorsDelay:         10,
		FindSuccessorRandomDelayMax: 10,
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
	if t.config.FindSuccessorRandomDelayMax > 0 {

		//TODO:
		//sleepTime := t.randSource.UInt63n(t.config.FindSuccessorRandomDelayMax)
	}
	return t.TCPTransport.FindSuccessors(vn, n, k, meta)
}

var _ = chord.Transport(&DelayedTCPTransport{})
