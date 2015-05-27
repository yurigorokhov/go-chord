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
	FirstTcpPort     = 9000
)

type NodeInfo struct {
	Ring      *chord.Ring
	Transport *chord.TCPTransport
}

func main() {

	// flags
	var simulationType = flag.String("type", "local", "type of simulation (local or distributed)")
	var simulationName = flag.String("name", "default", "name of this simulation, used to name stats in statsd")
	var join = flag.String("join", os.Getenv("JOIN"), "ip:port of ring to join, if empty start a new ring!")
	var listen = flag.String("listen", os.Getenv("LISTEN"), "port to listen on")
	var numNodes = flag.Int("numnodes", DefaultNodeCount, "number of nodes")
	flag.Parse()

	// parse simulation type
	switch *simulationType {
	case "local":
		LocalSimulation(*numNodes, *simulationName)
	case "distributed":
		RemoteSimulation(*join, *listen)
	default:
		fmt.Printf("Unknown simulation type: %s", *simulationType)
		os.Exit(1)
	}
}

func RemoteSimulation(join string, listen string) {

	// create a TCP transport
	tcpTimeout := time.Second * 5
	transport, err := chord.InitTCPTransport(listen, tcpTimeout)
	if err != nil {
		fmt.Printf("Error creating chord ring: %v", err)
		os.Exit(1)
	}
	fmt.Printf("\nLISTEN: %v\n", listen)
	conf := chord.DefaultConfig(listen)
	conf.StabilizeMin = time.Duration(15 * time.Millisecond)
	conf.StabilizeMax = time.Duration(3 * time.Second)
	conf.NumSuccessors = 1

	// if no ring was specified, we need to start one
	if len(join) == 0 {

		// create first host
		fmt.Printf("\nCreating ring %v\n", listen)
		_, err = chord.Create(conf, transport)
		if err != nil {
			fmt.Printf("Error creating chord ring: %v", err)
			os.Exit(1)
		}
	} else {

		fmt.Printf("\nJoining %v\n", join)
		_, err = chord.Join(conf, transport, join)
		if err != nil {
			fmt.Printf("Error joining chord ring: %v", err)
			os.Exit(1)
		}
	}

	// wait indefinitely
	<-make(chan bool)
}

func LocalSimulation(numberOfNodes int, simulationName string) {

	// collect stats
	stats := stats.NewPrintStats()
	defer stats.Print()

	// get a ring up and running!
	nodeMap := make(map[string]NodeInfo)
	port := FirstTcpPort
	tcpTimeout := time.Second * 5
	for i := 0; i < numberOfNodes; i++ {
		conf := chord.DefaultConfig(fmt.Sprintf(":%v", port))

		// we don't need to stabilize that often, since we are not joining/leaving nodes yet
		conf.StabilizeMin = time.Duration(15 * time.Millisecond)
		conf.StabilizeMax = time.Duration(1 * time.Second)
		conf.NumSuccessors = 1
		conf.Stats = stats

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
	}
	fmt.Printf("\nCreated %v nodes", numberOfNodes)
	fmt.Printf("\nWaiting %v seconds for the ring to settle\n", numberOfNodes)
	time.Sleep(time.Duration(int64(time.Second) * int64(numberOfNodes)))

	fmt.Printf("\nBeginning Simulation with name %s\n", simulationName)
	if err := RandomKeyLookups(nodeMap, 100); err != nil {
		fmt.Printf("\nError running simulation: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nSimulation finished")
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
