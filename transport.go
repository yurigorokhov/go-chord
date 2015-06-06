package chord

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Wraps vnode and object
type localRPC struct {
	vnode *Vnode
	obj   VnodeRPC
}

// Captures a delay and the probability of it occuring
type ProbabilityDelay struct {
	Probability float64
	Delay       uint64
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

// LocalTransport is used to provides fast routing to Vnodes running
// locally using direct method calls. For any non-local vnodes, the
// request is passed on to another transport.
type LocalTransport struct {
	host       string
	remote     Transport
	lock       sync.RWMutex
	local      map[string]*localRPC
	FakeTcp    bool
	config     *DelayTCPConfig
	randSource *rand.Rand
}

// Creates a local transport to wrap a remote transport
func InitLocalTransport(remote Transport) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[string]*localRPC)
	return &LocalTransport{remote: remote, local: local, FakeTcp: false}
}

func InitLocalTransportFakeTcp(remote Transport, conf *DelayTCPConfig) Transport {
	// Replace a nil transport with black hole
	if remote == nil {
		remote = &BlackholeTransport{}
	}

	local := make(map[string]*localRPC)
	return &LocalTransport{remote: remote, local: local, FakeTcp: true, config: conf, randSource: rand.New(rand.NewSource(time.Now().Unix()))}
}

// Checks for a local vnode
func (lt *LocalTransport) get(vn *Vnode) (VnodeRPC, bool) {
	key := vn.String()
	lt.lock.RLock()
	defer lt.lock.RUnlock()
	w, ok := lt.local[key]
	if ok {
		return w.obj, ok
	} else {
		return nil, ok
	}
}

func (lt *LocalTransport) ListVnodes(host string) ([]*Vnode, error) {
	// Check if this is a local host
	if host == lt.host || lt.FakeTcp {
		// Generate all the local clients
		res := make([]*Vnode, 0, len(lt.local))

		// Build list
		lt.lock.RLock()
		for _, v := range lt.local {
			res = append(res, v.vnode)
		}
		lt.lock.RUnlock()

		return res, nil
	}

	// Pass onto remote
	return lt.remote.ListVnodes(host)
}

func (lt *LocalTransport) Ping(vn *Vnode) (bool, error) {
	// Look for it locally
	_, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return true, nil
	}

	// Pass onto remote
	return lt.remote.Ping(vn)
}

func (lt *LocalTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.GetPredecessor()
	}

	// Pass onto remote
	return lt.remote.GetPredecessor(vn)
}

func (lt *LocalTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.Notify(self)
	}

	// Pass onto remote
	return lt.remote.Notify(vn, self)
}

func (lt *LocalTransport) FindSuccessors(vn *Vnode, n int, key []byte, meta LookupMetaData) (LookupMetaData, []*Vnode, error) {
	if lt.config != nil && lt.config.FindSuccessorsDelay > 0 {
		time.Sleep(time.Duration(lt.config.FindSuccessorsDelay * uint64(time.Millisecond)))
	}
	if lt.config != nil && len(lt.config.RandomDelays) > 0 {

		// pick a random number in the range [0,1)
		r := lt.randSource.Float64()

		// find the maximum probability that is smaller than rand, use the delay that corresponds to that probability
		j := -1
		for i, d := range lt.config.RandomDelays {
			if d.Probability > r && (j == -1 || d.Probability < lt.config.RandomDelays[j].Probability) {
				j = i
			}
		}
		if j > -1 {
			time.Sleep(time.Duration(lt.config.RandomDelays[j].Delay * uint64(time.Millisecond)))
		}
	}

	// Look for it locally
	obj, ok := lt.get(vn)

	// If it exists locally, handle it
	if ok {
		return obj.FindSuccessors(n, key, meta)
	}

	// Pass onto remote
	return lt.remote.FindSuccessors(vn, n, key, meta)
}

func (lt *LocalTransport) ClearPredecessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.ClearPredecessor(self)
	}

	// Pass onto remote
	return lt.remote.ClearPredecessor(target, self)
}

func (lt *LocalTransport) SkipSuccessor(target, self *Vnode) error {
	// Look for it locally
	obj, ok := lt.get(target)

	// If it exists locally, handle it
	if ok {
		return obj.SkipSuccessor(self)
	}

	// Pass onto remote
	return lt.remote.SkipSuccessor(target, self)
}

func (lt *LocalTransport) Register(v *Vnode, o VnodeRPC) {
	// Register local instance
	key := v.String()
	lt.lock.Lock()
	lt.host = v.Host
	lt.local[key] = &localRPC{v, o}
	lt.lock.Unlock()

	// Register with remote transport
	lt.remote.Register(v, o)
}

func (lt *LocalTransport) Deregister(v *Vnode) {
	key := v.String()
	lt.lock.Lock()
	delete(lt.local, key)
	lt.lock.Unlock()
}

// BlackholeTransport is used to provide an implemenation of the Transport that
// does not actually do anything. Any operation will result in an error.
type BlackholeTransport struct {
}

func (*BlackholeTransport) ListVnodes(host string) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", host)
}

func (*BlackholeTransport) Ping(vn *Vnode) (bool, error) {
	return false, nil
}

func (*BlackholeTransport) GetPredecessor(vn *Vnode) (*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s.", vn.String())
}

func (*BlackholeTransport) Notify(vn, self *Vnode) ([]*Vnode, error) {
	return nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (*BlackholeTransport) FindSuccessors(vn *Vnode, n int, key []byte, meta LookupMetaData) (LookupMetaData, []*Vnode, error) {
	return NewLookupMetaData(), nil, fmt.Errorf("Failed to connect! Blackhole: %s", vn.String())
}

func (*BlackholeTransport) ClearPredecessor(target, self *Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (*BlackholeTransport) SkipSuccessor(target, self *Vnode) error {
	return fmt.Errorf("Failed to connect! Blackhole: %s", target.String())
}

func (*BlackholeTransport) Register(v *Vnode, o VnodeRPC) {
}
