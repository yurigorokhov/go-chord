package main

import (
	"errors"
	"fmt"
	"go-chord"
	"go-chord/stats"
	"math/rand"
	"os"
	"time"
)

const (
	NodeCount    = 16
	FirstTcpPort = 9020
)

type NodeInfo struct {
	Ring      *chord.Ring
	Transport *chord.TCPTransport
}

func main() {

	// get a ring up and running!
	nodeMap := make(map[string]NodeInfo)
	port := FirstTcpPort
	tcpTimeout := time.Second * 5
	for i := 0; i < NodeCount; i++ {
		conf := chord.DefaultConfig(fmt.Sprintf(":%v", port))

		// we don't need to stabilize that often, since we are not joining/leaving nodes yet
		conf.StabilizeMin = time.Duration(15 * time.Millisecond)
		conf.StabilizeMax = time.Duration(1000 * time.Millisecond)
		conf.NumSuccessors = 1
		conf.Stats = &stats.PrintStats{}

		// 2 virtual nodes per physical node
		conf.NumVnodes = 2
		var r *chord.Ring
		var err error

		// create a TCP transport
		transport, err := chord.InitTCPTransport(fmt.Sprintf(":%v", port), tcpTimeout)
		if err != nil {
			fmt.Printf("Error creating chord ring: %v", err)
			os.Exit(1)
		}
		if i == 0 {

			// create first host
			r, err = chord.Create(conf, transport)
			if err != nil {
				fmt.Printf("Error creating chord ring: %v", err)
				os.Exit(1)
			}
		} else {

			// join the first host
			r, err = chord.Join(conf, transport, fmt.Sprintf(":%v", FirstTcpPort))
			if err != nil {
				fmt.Printf("Error joining chord ring: %v", err)
				os.Exit(1)
			}
		}
		nodeMap[conf.Hostname] = NodeInfo{
			Ring:      r,
			Transport: transport,
		}
		port++
		fmt.Printf("\nCreated node at %v", conf.Hostname)
	}
	fmt.Println("\nWaiting 15 seconds for the ring to settle")
	time.Sleep(15 * time.Second)

	fmt.Println("\nBeginning Simulation")
	if err := RandomKeyLookups(nodeMap, 100); err != nil {
		fmt.Printf("\nError running simulation: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// Performs lookupCount random key lookups
func RandomKeyLookups(nodes map[string]NodeInfo, lookupCount int) error {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < lookupCount; i++ {

		// generate random lookup value
		val := []byte(string(r.Int63n(int64(lookupCount))))

		// ask each node to perform the lookup, and ensure the result is the same!
		result := ""
		for _, v := range nodes {
			resultNodes, err := v.Ring.Lookup(1, val)
			if err != nil {
				fmt.Printf("\nError during lookup %v: %v", i, err)
				continue
			}
			if len(resultNodes) != 1 {
				fmt.Printf("\nExpected exactly 1 node to contain the value")
				continue
			}
			if result == "" {
				result = resultNodes[0].Host
			} else if result != resultNodes[0].Host {
				return errors.New("Inconsistent node hashing!")
			}
		}
	}
	return nil
}
