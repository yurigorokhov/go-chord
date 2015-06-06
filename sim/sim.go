package main

import (
	"errors"
	"flag"
	"fmt"
	"go-chord"
	"go-chord/stats"
	"math/rand"
	"os"
	"runtime"
	"time"
)

const (
	DefaultNodeCount = 16
	FirstTcpPort     = 9020
	TcpDelay         = 10
)

type nodeInfo struct {
	Ring      *chord.Ring
	Transport chord.Transport
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	// flags
	var numNodes = flag.Int("numnodes", DefaultNodeCount, "number of nodes")
	var tcpDelay = flag.Int("tcpdelay", TcpDelay, "tcp delay in milliseconds")
	var randDelayConfig = flag.String("randdelayconfig", "", "'200:.1|300:.2|500:.3' means delay 200ms 10% of the time, 300ms 20% of the time")
	var useCache = flag.Bool("usecache", false, "use the node cache or not")
	var fakeTcp = flag.Bool("faketcp", false, "fake the tcp connection")
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
	fmt.Print("Starting ring ")
	nodeMap := make(map[string]nodeInfo)
	port := FirstTcpPort
	tcpTimeout := time.Second * 30

	// Initialize a transport that knows all nodes
	localTransport := chord.InitLocalTransportFakeTcp(nil, delayConf)
	for i := 0; i < *numNodes; i++ {
		conf := chord.DefaultConfig(fmt.Sprintf(":%v", port))

		// we don't need to stabilize that often, since we are not joining/leaving nodes yet
		if *fakeTcp {
			conf.StabilizeMin = 10 * time.Millisecond
			conf.StabilizeMax = 20 * time.Millisecond
		} else {
			conf.StabilizeMin = 1 * time.Second
			conf.StabilizeMax = 3 * time.Second
		}
		conf.NumSuccessors = 1
		conf.Stats = stats
		conf.UseCache = *useCache

		// 2 virtual nodes per physical node
		conf.NumVnodes = 2
		var r *chord.Ring
		var err error

		// create a TCP transport
		var transport chord.Transport
		if *fakeTcp {
			transport = chord.InitLocalTransport(localTransport)
		} else {
			transport, err = InitDelayedTCPTransport(fmt.Sprintf(":%v", port), tcpTimeout, delayConf)
			if err != nil {
				fmt.Printf("Error creating chord ring: %v", err)
				os.Exit(1)
			}
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
		if !*fakeTcp {
			time.Sleep(5 * time.Second)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Print(".")
		port++
	}
	fmt.Printf("\nCreated %v nodes", *numNodes)
	if !(*fakeTcp) {
		fmt.Printf("\nWaiting %v seconds for the ring to settle\n", *numNodes*2)
		time.Sleep(time.Duration(int64(time.Second) * int64(*numNodes*2)))
	} else {
		fmt.Printf("\nWaiting %v milliseconds for the ring to settle\n", *numNodes*2)
		time.Sleep(time.Duration(int64(time.Millisecond) * int64(*numNodes*2)))
	}
	fmt.Printf("\nBeginning Simulation w/ general TCP delay of %vms, and TCP random delay of %v, and caching:%v\n",
		delayConf.FindSuccessorsDelay, *randDelayConfig, *useCache)
	if err := RandomKeyLookups(nodeMap, 50); err != nil {
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

		// pick 10 random nodes and ask each node to perform the lookup, and ensure the result is the same!
		result := ""
		for k := 0; k < 10; k++ {

			// Pick a random node
			randNode := r.Int63n(int64(len(nodes)))
			var j int64 = 0
			for _, n := range nodes {
				if j == randNode {
					resultNodes, err := n.Ring.Lookup(1, val)
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
					break
				}
				j++
			}
		}
		fmt.Print(".")
	}
	fmt.Println("\n")
	return nil
}
