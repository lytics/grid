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
	out := make(chan Event, 100)

	for i := 0; i < mgrsCnt; i++ {
		in := p.client(out)
		mgr := NewManager(i, topic, mgrsCnt)
		mgr.in = in
		mgr.out = out
		mgr.exit = exit
		managers = append(managers, mgr)
		go mgr.stateMachine()
	}

	leader := managers[0]
	evt := NewWritable(topic, Key, newPing(leader.name, 1))
	p.write(evt)

	time.Sleep(1 * time.Second)

	//The head of the partition should be a GridState Message
	inmsg := p.data[p.head-1].(Event)
	var cmdmsg *CmdMesg

	// Extract command message.
	switch msg := inmsg.Message().(type) {
	case *CmdMesg:
		cmdmsg = msg
	default:
		t.Logf("unknown type %T", msg)
	}

	// Check for type of message.
	switch data := cmdmsg.Data.(type) {
	case GridState:
		t.Logf("The head of the partition is a gridstate message as expected. %v", data)
	default:
		t.Fatalf("unknown type %T", data)
	}

	for _, mgr := range managers {
		if !reflect.DeepEqual(mgr.gstate, leader.gstate) {
			t.Fatalf("peers have missmatch states.  \n%v \nNot Equal \n%v", mgr.gstate.String(), leader.gstate.String())
		}
	}
}
