package grid

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"time"
)

const (
	Key          = "0"
	Skew         = 20
	TickMillis   = 500
	HeartTimeout = 6
	ElectTimeout = 20
)

const (
	Follower = iota
	Candidate
	Leader
)

// Ping is sent only by elected leader.
type Ping struct {
	Leader int
}

// Vote is sent by any voter in response to an election.
type Vote struct {
	Term      uint32
	Candidate int
	From      int
}

// Election is started by any voter that has timed out the leader.
type Election struct {
	Term      uint32
	Votes     uint32
	Candidate int
}

func init() {
	gob.Register(Ping{})
	gob.Register(Vote{})
	gob.Register(Election{})
}

func newPing(leader int) *CmdMesg {
	return &CmdMesg{Data: Ping{Leader: leader}}
}

func (p Ping) String() string {
	return fmt.Sprintf("Ping{Leader: %d}", p.Leader)
}

func newVote(candidate int, term uint32, from int) *CmdMesg {
	return &CmdMesg{Data: Vote{Term: term, Candidate: candidate, From: from}}
}

func (v Vote) String() string {
	return fmt.Sprintf("Vote{Term: %d, Candidate: %d, From: %d}", v.Term, v.Candidate, v.From)
}

func newElection(candidate int, term uint32) *CmdMesg {
	return &CmdMesg{Data: Election{Term: term, Votes: 1, Candidate: candidate}}
}

func (e Election) Copy() *Election {
	return &Election{Term: e.Term, Votes: e.Votes, Candidate: e.Candidate}
}

func (e Election) String() string {
	return fmt.Sprintf("Election{Term: %d, Votes: %d, Candidate: %d}", e.Term, e.Votes, e.Candidate)
}

func startVoter(name int, topic string, quorum uint32, maxleadertime int64, in <-chan Event) <-chan Event {
	out := make(chan Event, 0)
	go func() {
		defer close(out)
		voter(name, topic, quorum, maxleadertime, in, out)
	}()
	return out
}

// voter implements a RAFT election voter. It requires that the messages
// sent to the output channel have the same total-ordering for all
// voters participating in the elections.
//
// The basic sketch of an election:
//
//     1. Start as a Follower
//     2. Only leaders can send Ping messages, if voter does
//        not receive Ping message in HeartTimeout seconds
//        assume leader is dead. Move to Candidate state
//        and issue a new Election.
//     3. Upon receiving an Election, vote, but never twice for
//        the same election.
//     4. Upon receiving a Vote check if it's Candidate and Term
//        match the Candidate and Term of the current election,
//        and upvote if they do.
//     5. If a voter wins an election, that voter will move to
//        Leader state and start sending Ping, all other voters
//        move back to Follower.
//
// A much better explanation is at: http://thesecretlivesofdata.com/raft/
//
// Testing Notes:
//     The parameter 'maxleadertime' is a testing hook, to force
//     leaders to give up leadership in very short time periods.
//     In normal running, set it to 0 to disable.
//
func voter(name int, topic string, quorum uint32, maxleadertime int64, in <-chan Event, out chan<- Event) {
	ticker := time.NewTicker(TickMillis * time.Millisecond)
	defer ticker.Stop()

	voted := make(map[uint32]bool)
	rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	state := Follower

	lasthearbeat := time.Now().Unix()
	nextelection := time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)

	var termstart int64
	var elect *Election
	var term uint32
	for {
		select {
		case now := <-ticker.C:
			if now.Unix()-lasthearbeat > HeartTimeout {
				if state != Follower {
					log.Printf("voter %v: transition: %v => %v", name, state, Follower)
				}
				state = Follower
				elect = nil
			}
			if state == Leader && (time.Now().Unix() < termstart+maxleadertime || maxleadertime == 0) {
				out <- NewWritable(topic, Key, newPing(name))
			}
			if time.Now().Unix() > nextelection && state == Follower {
				if state != Candidate {
					log.Printf("voter %v: transition: %v => %v", name, state, Candidate)
				}
				state = Candidate
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				out <- NewWritable(topic, Key, newElection(name, term))
			}
		case m := <-in:
			var cmdmsg *CmdMesg

			switch msg := m.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				continue
			}

			switch data := cmdmsg.Data.(type) {
			case Ping:
				// log.Printf("%v rx: %v", name, data)
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				if name != data.Leader {
					state = Follower
				}
			case Vote:
				// log.Printf("%v rx: %v", name, data)
				if elect == nil {
					continue
				}
				if elect.Candidate != data.From && elect.Candidate == data.Candidate && elect.Term == data.Term {
					elect.Votes = elect.Votes + 1
				}
				if elect.Votes >= quorum && elect.Candidate == name {
					if state != Leader {
						log.Printf("voter %v: transition: %v => %v", name, state, Leader)
					}
					state = Leader
					termstart = time.Now().Unix()
				}
				if elect.Votes >= quorum && elect.Candidate != name {
					state = Follower
				}
				if elect.Votes >= quorum {
					lasthearbeat = time.Now().Unix()
					nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				}
			case Election:
				// log.Printf("%v rx: %v", name, data)
				if elect != nil {
					continue
				}
				elect = data.Copy()
				term++
				if !voted[data.Term] && state != Leader {
					voted[data.Term] = true
					out <- NewWritable(topic, Key, newVote(data.Candidate, data.Term, name))
				}
			default:
				log.Printf("%v rx: unknonw type %T :: %v", name, data, data)
			}
		}
	}
}
