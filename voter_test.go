package grid

import (
	"sync"
	"testing"
	"time"
)

func TestElectionOf1(t *testing.T) {

	const (
		maxleadertime = 10 // Test hook, use 0 for normal use.
		voters        = 1
		quorum        = 1
		topic         = "test-election"
	)

	p := newPartition()
	wg := new(sync.WaitGroup)
	wg.Add(voters)

	for i := 0; i < voters; i++ {
		out := make(chan Event)
		in := p.client(out)
		go voter(i, topic, quorum, maxleadertime, in, out)
	}
	time.Sleep(45 * time.Second)

	// Election is finished, now check the partition data
	// to see what sequence of messages were sent between
	// voters.

	if p.data[0] == nil {
		t.Fatalf("no messages found")
	}

	switch data := p.data[0].Message().(type) {
	case Election:
	default:
		t.Fatalf("first message should of been: Election but was: %v", data)
	}

	leaderelected := false
	votes := make(map[int]int)
	for i := 1; i < p.head; i++ {
		switch data := p.data[i].Message().(type) {
		case Election:
		case Vote:
			votes[data.Candidate] = 1 + votes[data.Candidate]
		case Ping:
			if !isleaderelected(quorum, votes) {
				t.Fatalf("found ping from leader, but no leader elected: %v: votes: %v", data, votes)
			}
			leaderelected = true
		default:
			t.Fatalf("found unknown message type %T :: %v", data, data)
		}
	}

	if !leaderelected {
		t.Fatalf("failed to elect leader")
	}
}

func TestElectionOf3(t *testing.T) {

	const (
		maxleadertime = 10 // Test hook, use 0 for normal use.
		voters        = 3
		quorum        = 2
		topic         = "test-election"
	)

	p := newPartition()
	wg := new(sync.WaitGroup)
	wg.Add(voters)

	for i := 0; i < voters; i++ {
		out := make(chan Event)
		in := p.client(out)
		go voter(i, topic, quorum, maxleadertime, in, out)
	}
	time.Sleep(45 * time.Second)

	// Election is finished, now check the partition data
	// to see what sequence of messages were sent between
	// voters.

	if p.data[0] == nil {
		t.Fatalf("no messages found")
	}

	switch data := p.data[0].Message().(type) {
	case Election:
	default:
		t.Fatalf("first message should of been: Election but was: %v", data)
	}

	votes := make(map[int]int)
	for i := 1; i < p.head; i++ {
		switch data := p.data[i].Message().(type) {
		case Election:
		case Vote:
			votes[data.Candidate] = 1 + votes[data.Candidate]
		case Ping:
			if !isleaderelected(quorum, votes) {
				t.Fatalf("found ping from leader, but no leader elected: %v: votes: %v", data, votes)
			}
		default:
			t.Fatalf("found unknown message type %T :: %v", data, data)
		}
	}
}

// isleaderelected returns true if the votes reflect that a leader was
// elected for the given quorum.
func isleaderelected(quorum int, votes map[int]int) bool {
	for _, v := range votes {
		if v >= quorum {
			return true
		}
	}
	return false
}

// partition is a mock implementation of a Kafka partition.
// It gives a total ordering to incoming messages, and
// operates as a log readable by multiple clients.
type partition struct {
	head  int
	data  []Event
	mutex *sync.Mutex
}

// newPartition creates a new partition.
func newPartition() *partition {
	return &partition{data: make([]Event, 1000000), mutex: new(sync.Mutex)}
}

// client creates a "client" for the partition. A client
// in this case is a readable channel from the caller
// which will be read by the client and produced onto
// the partition, and a readbale channel returned
// to the caller of consumable messages from the
// partition.
func (p *partition) client(in <-chan Event) <-chan Event {
	out := make(chan Event, 100)
	go func(out chan<- Event) {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		offset := 0
		for {
			select {
			case <-ticker.C:
				m := p.read(offset)
				if m != nil {
					offset++
					out <- m
				}
			case m := <-in:
				p.write(m)
			}
		}
	}(out)
	// Our output is someone elses input.
	return out
}

// read reads from the partition a particular offset.
func (p *partition) read(offset int) Event {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if offset < p.head {
		return p.data[offset]
	} else {
		return nil
	}
}

// write writes to the end of the partition, unless it is
// out of space.
func (p *partition) write(m Event) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.head < 1000000 {
		p.data[p.head] = m
		p.head++
	} else {
		panic("out of space")
	}
}
