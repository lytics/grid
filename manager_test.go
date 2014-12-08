package grid

import (
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestManager(t *testing.T) {

	const (
		topic      = "test-election"
		managercnt = 3
	)

	g, err := New("test-grid", managercnt)
	if err != nil {
		t.Fatalf("failed to create grid: %v", err)
	}

	// Set the read-write log to no-op log, otherwise
	// the test will fail with errors related to
	// kafka message encoding/decodeing, which are
	// unrelated for the purposes of this test.
	g.log = newNoOpReadWriteLog()

	f := func(in <-chan Event) <-chan Event { return nil }
	p := newPartition()
	managers := make([]*Manager, 0)

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	ops := make(map[string]*op)
	ops["f1"] = &op{f: f, n: 2, inputs: topics}

	for i := 0; i < managercnt; i++ {
		out := make(chan Event)
		in := p.client(out)

		mgr := NewManager(i, g)
		mgr.peertimeout = 5000 // We don't want the peers timing out for this test
		go mgr.stateMachine(in, out)

		mgr.ops = ops
		mgr.parts = parts

		managers = append(managers, mgr)
	}

	leader := managers[0]
	p.write(NewWritable(topic, Key, newPing(leader.name, 1)))

	time.Sleep(1 * time.Second)

	// The head of the partition should be a PeerState message.
	inmsg := p.data[p.head-1].(Event)
	var cmdmsg *CmdMesg

	// Extract command message.
	switch msg := inmsg.Message().(type) {
	case *CmdMesg:
		cmdmsg = msg
	default:
		t.Logf("unknown type :%T, psize:%v", msg, p.head)
	}

	// Check for type of message.
	switch data := cmdmsg.Data.(type) {
	case PeerState:
		t.Logf("The head of the partition is a gridstate message as expected. %v", data)
	default:
		t.Fatalf("unknown type:%T, psize:%v", data, p.head)
	}

	// Ensure all the managers have the same grid state.
	for _, mgr := range managers {
		if !reflect.DeepEqual(mgr.state, leader.state) {
			t.Fatalf("peers have mismatch states.  \n%v \nNot Equal \n%v", mgr.state, leader.state)
		}
	}
}

func TestManagerGridDeath(t *testing.T) {

	const (
		topic      = "test-election"
		managercnt = 3
	)

	g, err := New("test-grid", managercnt)
	if err != nil {
		t.Fatalf("failed to create grid: %v", err)
	}

	// Set the read-write log to no-op log, otherwise
	// the test will fail with errors related to
	// kafka message encoding/decodeing, which are
	// unrelated for the purposes of this test.
	g.log = newNoOpReadWriteLog()

	f := func(in <-chan Event) <-chan Event { return nil }
	p := newPartition()
	managers := make([]*Manager, 0)

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	ops := make(map[string]*op)
	ops["f1"] = &op{f: f, n: 2, inputs: topics}

	for i := 0; i < managercnt; i++ {
		out := make(chan Event)
		in := p.client(out)

		mgr := NewManager(i, g)
		mgr.tkohander = func() {
			t.Fatalf("The managers shouldn't have exited yet.")
		}
		mgr.peertimeout = 1 // timeout fast
		go mgr.stateMachine(in, out)

		mgr.ops = ops
		mgr.parts = parts

		managers = append(managers, mgr)
	}

	abortpinger := make(chan bool)

	leader := managers[0]
	go func() {
		ticker := time.NewTicker(TickMillis * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.write(NewWritable(topic, Key, newPing(leader.name, 1)))
			case <-abortpinger:
				t.Log("mock leader is aborting.")
				return
			}
		}
	}()

	time.Sleep(2 * time.Second) // let the cluster run longer than the current peer timeout(1 sec) to ensure it remains running.

	//Now we can replace the tko handler so we can count the nodes as they exit.
	var deadNodes uint64 = 0
	for _, mgr := range managers {
		mgr.tkohander = func() {
			atomic.AddUint64(&deadNodes, 1)
		}
	}

	// Now stop the mock leader, which should kill the cluster.
	abortpinger <- true
	time.Sleep(2 * time.Second) // lets the cluster run it should dead off since we killed the pinger.
	deadNodesFinal := atomic.LoadUint64(&deadNodes)

	if deadNodesFinal != managercnt {
		t.Fatalf("Dead nodes not equal to manager nodes.   %v != %v", deadNodesFinal, managercnt)
	}

}
