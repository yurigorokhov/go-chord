package main

import (
	"errors"
	"flag"
	"fmt"
	"go-chord"
	"go-chord/stats"
	"math/rand"
	"os"
	"time"
)

const (
	DefaultNodeCount = 16
	FirstTcpPort     = 9020
	TcpDelay         = 10
)

type nodeInfo struct {
	Ring      *chord.Ring
	Transport *DelayedTCPTransport
}

func main() {

	// flags
	var simulationName = flag.String("name", "default", "name of this simulation, used to name stats in statsd")
	var numNodes = flag.Int("numnodes", DefaultNodeCount, "number of nodes")
	var tcpDelay = flag.Int("tcpdelay", TcpDelay, "tcp delay in milliseconds")
	var randDelayConfig = flag.String("randdelayconfig", "", "'200:.1|300:.2|500:.3' means delay 200ms 10% of the time, 300ms 20% of the time")
	var useCache = flag.Bool("usecache", false, "use the node cache or not")
	flag.Parse()

	// collect stats
	stats := stats.NewPrintStats()
	defer stats.Print()

	// delay config
	delayConf := DefaultDelayConfig()
	delayConf.FindSuccessorsDelay = uint64(*tcpDelay)
	if *randDelayConfig != "" {
		randDelays, err := NewProbabilityDelaysFromStr(*randDelayConfig)
		if err != nil {
			fmt.Printf("\nError parsing random delay config: %v", err)
			os.Exit(1)
		}
		delayConf.RandomDelays = randDelays
	}

	// get a ring up and running!
	nodeMap := make(map[string]nodeInfo)
	port := FirstTcpPort
	tcpTimeout := time.Second * 30
	for i := 0; i < *numNodes; i++ {
		conf := chord.DefaultConfig(fmt.Sprintf(":%v", port))

		// we don't need to stabilize that often, since we are not joining/leaving nodes yet
		conf.StabilizeMin = time.Duration(1 * time.Millisecond)
		conf.StabilizeMax = time.Duration(uint64(*numNodes) * uint64(time.Millisecond) * delayConf.MaxPossibleDelay() * 10)
		conf.NumSuccessors = 1
		conf.Stats = stats
		conf.UseCache = *useCache

		// 2 virtual nodes per physical node
		// TODO(yurig): can we get this down to 1?
		conf.NumVnodes = 2
		var r *chord.Ring
		var err error

		// create a TCP transport
		transport, err := InitDelayedTCPTransport(fmt.Sprintf(":%v", port), tcpTimeout, delayConf)
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
		nodeMap[conf.Hostname] = nodeInfo{
			Ring:      r,
			Transport: transport,
		}
		port++
	}
	fmt.Printf("\nCreated %v nodes", *numNodes)
	fmt.Printf("\nWaiting %v seconds for the ring to settle\n", *numNodes*2)
	time.Sleep(time.Duration(int64(time.Second) * int64(*numNodes*2)))
	fmt.Printf("\nBeginning Simulation with name %s and general TCP delay of %vms, and TCP random delay of %v\n",
		*simulationName, delayConf.FindSuccessorsDelay, *randDelayConfig)
	if err := RandomKeyLookups(nodeMap, 100); err != nil {
		fmt.Printf("\nError running simulation: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nSimulation finished")
}

// Performs lookupCount random key lookups
func RandomKeyLookups(nodes map[string]nodeInfo, lookupCount int) error {
	fmt.Println("\n")
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
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Print(".")
	}
	fmt.Println("\n")
	return nil
}
