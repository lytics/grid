package grid

import (
	"reflect"
	"testing"
	"time"
)

func TestManager(t *testing.T) {

	const (
		topic   = "test-election"
		mgrsCnt = 3
	)

	p := newPartition()
	exit := make(chan bool)

	managers := make([]*Manager, 0)

	for i := 0; i < mgrsCnt; i++ {
		mgr := NewManager(i, topic, mgrsCnt)
		in := p.client(mgr.Events())
		mgr.startStateMachine(in, exit)
		managers = append(managers, mgr)
	}

	leader := managers[0]
	evt := NewWritable(topic, Key, newPing(leader.Name, 1))
	leader.out <- evt //Have the leader send out its first leader ping.

	time.Sleep(1 * time.Second)

	//The head of the partition should be a GridState Message
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
	case GridState:
		t.Logf("The head of the partition is a gridstate message as expected. %v", data)
	default:
		t.Fatalf("unknown type:%T, psize:%v", data, p.head)
	}

	//Ensure all the managers have the same grid state.
	for _, mgr := range managers {
		if !reflect.DeepEqual(mgr.gstate, leader.gstate) {
			t.Fatalf("peers have mismatch states.  \n%v \nNot Equal \n%v", mgr.gstate.String(), leader.gstate.String())
		}
	}
}
