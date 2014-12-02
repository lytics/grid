package grid

import (
	"sync"
	"testing"
	"time"
)

func TestElectionOf1(t *testing.T) {

	const (
		maxleadertime = 10 // Test hook, use 0 for normal use.
		topic         = "test-election"
		voters        = 1
		quorum        = 1
	)

	p := newPartition()
	exit := make(chan bool)

	for i := 0; i < voters; i++ {
		out := make(chan Event)
		in := p.client(out)
		go voter(i, topic, quorum, maxleadertime, in, out, exit)
	}
	time.Sleep(45 * time.Second)

	// Election is finished, now check the partition data
	// to see what sequence of messages were sent between
	// voters.

	isleaderelected(t, p, quorum)
	close(exit)
}

func TestElectionOf3(t *testing.T) {

	const (
		maxleadertime = 10 // Test hook, use 0 for normal use.
		topic         = "test-election"
		voters        = 3
		quorum        = 2
	)

	p := newPartition()
	exit := make(chan bool)

	for i := 0; i < voters; i++ {
		out := make(chan Event)
		in := p.client(out)
		go voter(i, topic, quorum, maxleadertime, in, out, exit)
	}
	time.Sleep(45 * time.Second)

	// Election is finished, now check the partition data
	// to see what sequence of messages were sent between
	// voters.

	isleaderelected(t, p, quorum)
	close(exit)
}

// validateElection checks the partition log for all messages passed
// and asserts that they happend in the expected order with the
// expected result of a leader being elected.
func isleaderelected(t *testing.T, p *partition, quorum int) {

	// sufficient votes to become leader simply means that there
	// where 'quorum' number of votes cast for a particular
	// candidate.
	sufficientvotes := func(quorum int, votes map[string]int) bool {
		for _, v := range votes {
			if v >= quorum {
				return true
			}
		}
		return false
	}

	// Fatal if no messages at all were send by any voter.
	if p.data[0] == nil {
		t.Fatalf("no messages found")
	}

	var cmdmsg *CmdMesg

	// First message should be a command-message, the container
	// type for the command topic.
	switch msg := p.data[0].Message().(type) {
	case *CmdMesg:
		cmdmsg = msg
	default:
		t.Fatalf("first message should of been: CmdMesg, but was: %v", msg)
	}

	// The contents of the first command-message should be an
	// election request, this is becuase when the system
	// starts no leader exists, eventually the voters will
	// timeout their leader heartbeats, and one of them
	// should be the first to send out an election request.
	switch data := cmdmsg.Data.(type) {
	case Election:
	default:
		t.Fatalf("first message should of contained: Election, but was: %v", data)
	}

	leaderelected := false
	votes := make(map[string]int)
	for i := 1; i < p.head; i++ {
		var cmdmsg *CmdMesg

		// Skip over anything that is not a command-message,
		// since elections don't involve any other type
		// of message.
		switch msg := p.data[i].Message().(type) {
		case *CmdMesg:
			cmdmsg = msg
		default:
			continue
		}

		// The validation is simple in the sense that we just check all the
		// votes and see if there were enough for any particular candidate
		// to become leader. This is a real "gross" validation and in the
		// future probably needs to be much more fine grained.
		switch data := cmdmsg.Data.(type) {
		case Election:
		case Vote:
			votes[data.Candidate] = 1 + votes[data.Candidate]
		case Ping:
			if !sufficientvotes(quorum, votes) {
				t.Fatalf("found ping from leader, but no leader elected: %v: votes: %v: quorum: %v", data, votes, quorum)
			}
			leaderelected = true
		default:
			t.Fatalf("found unknown message type %T :: %v", data, data)
		}
	}

	// If no leader was elected at all this is fatal.
	if !leaderelected {
		t.Fatalf("failed to elect leader")
	}
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
