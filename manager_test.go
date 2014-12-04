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

	f := func(in <-chan Event) <-chan Event { return nil }
	p := newPartition()
	exit := make(chan bool)
	managers := make([]*Manager, 0)

	topics := make(map[string]bool)
	topics["topic1"] = true
	topics["topic2"] = true

	parts := make(map[string][]int32)
	parts["topic1"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}
	parts["topic2"] = []int32{0, 1, 2, 3, 4, 5, 6, 7}

	ops := make(map[string]*op)
	ops["f1"] = &op{f: f, n: 2, inputs: topics}

	for i := 0; i < mgrsCnt; i++ {
		out := make(chan Event)
		in := p.client(out)

		mgr := NewManager(i, topic, mgrsCnt)
		go mgr.stateMachine(in, out, exit)

		mgr.ops = ops
		mgr.parts = parts

		managers = append(managers, mgr)
	}

	leader := managers[0]
	p.write(NewWritable(topic, Key, newPing(leader.name, 1)))

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
