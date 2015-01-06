package grid

import (
	"log"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type nilactor struct{}

func (a *nilactor) Act(in <-chan Event, state ...<-chan Event) <-chan Event {
	return nil
}

func newNilActor() NewActor {
	return func(name string, id int) Actor { return &nilactor{} }
}

func TestManagerBasic(t *testing.T) {

	const (
		topic      = "test-basic"
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

	p := newPartition()
	managers := make([]*Manager, 0)
	manager_state := make(map[string]string)

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	actorconfs := make(map[string]*actorconf)
	actorconfs["f1"] = &actorconf{af: newNilActor(), n: 2, inputs: topics}

	createManager := func(id int) *Manager {
		out := make(chan Event)
		in := p.client(out)

		mgr := NewManager(id, g)
		mgr.peertimeout = 5000 // We don't want the peers timing out for this test
		go mgr.stateMachine(in, out)

		mgr.actorconfs = actorconfs
		mgr.parts = parts

		managers = append(managers, mgr)
		return mgr
	}

	for i := 0; i < managercnt; i++ {
		createManager(i)
	}
	leader := managers[2]
	go mockLeader(leader, p, topic, manager_state)
	time.Sleep(2 * time.Second)

	// Ensure all the managers have the same grid state.
	for _, mgr := range managers {
		if len(mgr.state.Peers) != managercnt {
			t.Fatalf("peer %v should have a peer count of %v, but has %v", mgr.name, len(mgr.state.Peers), managercnt)
		}
		if !reflect.DeepEqual(mgr.state, leader.state) {
			t.Fatalf("peers have mismatch states.  \n%v \nNot Equal \n%v", mgr.state, leader.state)
		}
	}
}

func TestManagerGridDeath(t *testing.T) {

	const (
		topic      = "test-grid-death"
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

	p := newPartition()
	managers := make([]*Manager, 0)

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	actorconfs := make(map[string]*actorconf)
	actorconfs["f1"] = &actorconf{af: newNilActor(), n: 2, inputs: topics}

	for i := 0; i < managercnt; i++ {
		out := make(chan Event)
		in := p.client(out)

		mgr := NewManager(i, g)
		mgr.tkohandler = func() {
			t.Fatalf("The managers shouldn't have exited yet.")
		}
		mgr.peertimeout = 1 // timeout fast
		go mgr.stateMachine(in, out)

		mgr.actorconfs = actorconfs
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
		topic      = "test-rolling-restart"
		managercnt = 3
	)
	managers := make([]*Manager, 0)
	manager_state := make(map[string]string)

	g, err := New("test-grid", managercnt)
	if err != nil {
		t.Fatalf("failed to create grid: %v", err)
	}

	// Set the read-write log to no-op log, otherwise
	// the test will fail with errors related to
	// kafka message encoding/decodeing, which are
	// unrelated for the purposes of this test.
	g.log = newNoOpReadWriteLog()

	p := newPartition()

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	actorconfs := make(map[string]*actorconf)
	actorconfs["f1"] = &actorconf{af: newNilActor(), n: 2, inputs: topics}

	createManager := func(id int, peertimeout int64) {
		out := make(chan Event)
		exit := make(chan bool)
		in := p.client(out)

		mgr := NewManager(id, g)
		mgr.peertimeout = peertimeout
		mgr.exithook = exit
		go mgr.stateMachine(in, out)

		mgr.actorconfs = actorconfs
		mgr.parts = parts
		manager_state[mgr.name] = "alive"

		mgr.tkohandler = func() {
			log.Printf("-- mock tkohandler: peer %v died", mgr.name)
			manager_state[mgr.name] = "dead"
		}
		managers = append(managers, mgr)
	}

	for i := 0; i < managercnt; i++ {
		createManager(i, 1)
	}

	go mockLeader(managers[1], p, topic, manager_state)
	time.Sleep(2000 * time.Millisecond)

	//Make sure the cluster is still alive and has established an epoch
	for i := 0; i < 3; i++ {
		mgr := managers[i]
		if manager_state[mgr.name] == "dead" {
			t.Fatalf("The manager %v should be alive, not dead", mgr.name)
		}
		if mgr.epoch == 0 {
			t.Fatalf("The epoch should have changed for manager %v", mgr.name)
		}
	}

	//////////////////////////////////////////////////////////////////////
	//Do a rolling restart of the grid
	managers[2].exithook <- true // kill peeer1_1 who isn't the leader
	log.Printf("-- manager: peer %v killed by test harness", managers[2].name)
	manager_state[managers[2].name] = "dead"

	createManager(3, 1)
	time.Sleep(1000 * time.Millisecond)
	createManager(4, 1)
	time.Sleep(2000 * time.Millisecond)

	//The old grid (peers [0,1,2]) should be dead by now.
	for i := 0; i < 3; i++ {
		mgr := managers[i]
		if manager_state[mgr.name] == "alive" {
			t.Fatalf("The manager %v should be dead by now", mgr.name)
		}
	}

	go mockLeader(managers[4], p, topic, manager_state)
	createManager(5, 1)
	time.Sleep(2000 * time.Millisecond) // "-- done sleeping after new peer 5 started --"

	// Make sure the new grid (peers [3,4,5]) didn't accept any peers from the old grid (peers [0,1,2])
	// Also make sure the new grid is running
	for i := 3; i < 6; i++ {
		mgr := managers[i]
		for pname, _ := range mgr.state.Peers {
			if managers[0].name == pname || managers[1].name == pname || managers[2].name == pname {
				t.Fatalf("manager:%v: peernames from the old epoch are in the current epoch: state %v", mgr.name, mgr.state.Peers)
			}
		}
		if manager_state[mgr.name] == "dead" {
			t.Fatalf("The manager %v should be alive, not dead", mgr.name)
		}
		if mgr.epoch == 0 {
			t.Fatalf("The epoch should have changed for manager %v", mgr.name)
		}
	}

	// Make sure the old grid (peers [0,1,2]) didn't accept any peers from the new grid (peers [3,4,5])
	for i := 0; i < 3; i++ {
		mgr := managers[i]
		for pname, _ := range mgr.state.Peers {
			if managers[3].name == pname || managers[4].name == pname || managers[5].name == pname {
				t.Fatalf("manager:%v: peernames from the old epoch are in the current epoch: state %v", mgr.name, mgr.state.Peers)
			}
		}
	}
}

func mockLeader(mgr *Manager, p *partition, topic string, manager_state map[string]string) {
	out := make(chan Event, 100)
	in := p.client(out)
	pingmsg := NewWritable(topic, Key, newPing(0, mgr.name, 1))

	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	log.Printf("-- starting mock leader for : %v", mgr.name)
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
				continue
			}
			// Check for type of message.
			switch data := cmdmsg.Data.(type) {
			case PeerState:
				pingmsg = NewWritable(topic, Key, newPing(data.Epoch, mgr.name, 1))
				epoch = data.Epoch
			default:
			}
		case <-ticker.C:
			if manager_state[mgr.name] == "dead" {
				log.Printf("-- mock leader %v exiting because it's manager is in state 'dead'", mgr.name)
				return
			}
			p.write(pingmsg)
		}
	}
}
