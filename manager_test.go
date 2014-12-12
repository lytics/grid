package grid

import (
	"log"
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

	createManager := func(id int) *Manager {
		out := make(chan Event)
		in := p.client(out)

		mgr := NewManager(id, g)
		mgr.peertimeout = 5000 // We don't want the peers timing out for this test
		go mgr.stateMachine(in, out)

		mgr.ops = ops
		mgr.parts = parts

		managers = append(managers, mgr)
		return mgr
	}

	for i := 0; i < managercnt; i++ {
		createManager(i)
	}

	leader := managers[0]
	p.write(NewWritable(topic, Key, newPing(0, leader.name, 1)))

	time.Sleep(1 * time.Second)

	// The head of the partition should be a PeerState message.
	b := p.data[p.head-1]
	inmsg := p.toevent(b)

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
		mgr.tkohandler = func() {
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
				p.write(NewWritable(topic, Key, newPing(0, leader.name, 1)))
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
		mgr.tkohandler = func() {
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

func TestManagerRollingRestartOfGrid(t *testing.T) {

	const (
		topic      = "test-election"
		managercnt = 3
	)
	managers := make([]*Manager, 0)

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

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	ops := make(map[string]*op)
	ops["f1"] = &op{f: f, n: 2, inputs: topics}

	createManager := func(id int, peertimeout int64) {
		out := make(chan Event)
		exit := make(chan bool)
		in := p.client(out)

		mgr := NewManager(id, g)
		mgr.peertimeout = peertimeout
		mgr.exithook = exit
		go mgr.stateMachine(in, out)

		mgr.ops = ops
		mgr.parts = parts

		mgr.tkohandler = func() {
			log.Printf("-- peer %v died -------------------------------------------------------------", id)
		}
		managers = append(managers, mgr)
		t.Logf("created peer %v.", id)
	}

	for i := 0; i < managercnt; i++ {
		createManager(i, 1)
	}

	abortpinger := make(chan bool)
	leaderpinger := func(mgr *Manager) {
		out := make(chan Event, 100)
		in := p.client(out)
		pingmsg := NewWritable(topic, Key, newPing(0, mgr.name, 1))

		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		log.Printf("starting mock leader ")
		var epoch uint64 = 0
		for {
			select {
			case event := <-in:
				var cmdmsg *CmdMesg
				switch msg := event.Message().(type) {
				case *CmdMesg:
					cmdmsg = msg
				default:
					continue
				}

				if cmdmsg.Epoch != epoch {
					log.Printf("---mock leader %v: command message epoch mismatch %v != %v, msg: %v", mgr.name, cmdmsg.Epoch, epoch, cmdmsg.Data)
					continue
				}
				// Check for type of message.
				switch data := cmdmsg.Data.(type) {
				case PeerState:
					log.Printf("---mock leader %v: has gotten a PeerState message and is switching its epoch to %v", mgr.name, data.Epoch)
					pingmsg = NewWritable(topic, Key, newPing(data.Epoch, mgr.name, 1))
					epoch = data.Epoch
				default:
				}
			case <-ticker.C:
				p.write(pingmsg)
			case <-abortpinger:
				log.Printf("mock leader is aborting.")
				return
			}
		}
	}
	go leaderpinger(managers[1])

	sleep(4000*time.Millisecond, "-- done sleeping to let the cluster run in a healthy state for a bit --")
	//sleep(40000*time.Millisecond, "-- done sleeping to let the cluster run in a healthy state for a bit --")
	/*
		for _, mgr := range managers {
			if mgr.epoch == 0 {
				t.Fatalf("The epoch should have changed for manager %v", mgr.name)
			}
		}
		managers[2].exithook <- true // kill peeer1_1 who isn't the leader

		createManager(5500, 1)
		sleep(1000*time.Millisecond, "-- done sleeping after new peer 5500 started --")
		createManager(5501, 1)
		sleep(900*time.Millisecond, "-- done sleeping after new peer 5501 started --")
		go leaderpinger(managers[4])
		createManager(5502, 1)
		sleep(2000*time.Millisecond, "-- done sleeping after new peer 5502 started --")

		// Loop through all the new peers to make sure they never let any old peers join there epoch
		for i := 3; i < 6; i++ {
			mgr := managers[i]
			for pname, _ := range mgr.state.Peers {
				if managers[0].name == pname || managers[1].name == pname || managers[2].name == pname {
					t.Fatalf("manager:%v: peernames from the old epoch are in the current epoch: state %v", mgr.name, mgr.state.Peers)
				}
			}
		}

		// Loop through all the old peers to make sure they never let any news peers join there epoch
		for i := 0; i < 3; i++ {
			mgr := managers[i]
			for pname, _ := range mgr.state.Peers {
				if managers[3].name == pname || managers[4].name == pname || managers[5].name == pname {
					t.Fatalf("manager:%v: peernames from the old epoch are in the current epoch: state %v", mgr.name, mgr.state.Peers)
				}
			}
		}
	*/

}

func sleep(t time.Duration, msg string) {
	time.Sleep(t) // let the cluster run longer than the current peer timeout(1 sec) to ensure it remains running.
	log.Printf(msg)
}
