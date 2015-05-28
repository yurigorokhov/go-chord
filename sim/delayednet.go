package main

import (
	"go-chord"
	"time"
)

// Config for DelayedTCPTransport
// All delays are in milliseconds
type DelayTCPConfig struct {
	FindSuccessorsDelay uint64
}

// Create a new delay config with default values
func DefaultDelayConfig() *DelayTCPConfig {
	return &DelayTCPConfig{
		FindSuccessorsDelay: 20,
	}
}

/*
DelayedTCPTransport provides a TCP transport layer with contrived network delays.
This is for simulation only!
*/
type DelayedTCPTransport struct {
	*chord.TCPTransport
	config *DelayTCPConfig
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
	}, nil
}

func (t *DelayedTCPTransport) FindSuccessors(vn *chord.Vnode, n int, k []byte, meta chord.LookupMetaData) (chord.LookupMetaData, []*chord.Vnode, error) {
	if t.config.FindSuccessorsDelay > 0 {
		time.Sleep(time.Duration(t.config.FindSuccessorsDelay * uint64(time.Millisecond)))
	}
	return t.TCPTransport.FindSuccessors(vn, n, k, meta)
}

var _ = chord.Transport(&DelayedTCPTransport{})
