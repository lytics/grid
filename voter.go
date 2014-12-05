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

type Rank int

const (
	Follower Rank = iota
	Candidate
	Leader
)

// Ping is sent only by elected leader.
type Ping struct {
	Leader string
	Term   uint32
}

// Pong is send only by followers in response to a leader's Ping.
type Pong struct {
	Follower string
	Term     uint32
}

// Vote is sent by any voter in response to an election.
type Vote struct {
	Term      uint32
	Candidate string
	From      string
}

// Election is started by any voter that has timed out the leader.
type Election struct {
	Term      uint32
	Votes     uint32
	Candidate string
}

func init() {
	gob.Register(Ping{})
	gob.Register(Pong{})
	gob.Register(Vote{})
	gob.Register(Election{})
	gob.Register(GridState{})
	gob.Register(Peer{})
	gob.Register(PeerSched{})
	gob.Register(Instance{})
}

func newPing(leader string, term uint32) *CmdMesg {
	return &CmdMesg{Data: Ping{Leader: leader, Term: term}}
}

func (p Ping) String() string {
	return fmt.Sprintf("Ping{Leader: %v, Term: %d}", p.Leader, p.Term)
}

func newPong(follower string, term uint32) *CmdMesg {
	return &CmdMesg{Data: Pong{Follower: follower, Term: term}}
}

func (p Pong) String() string {
	return fmt.Sprintf("Pong{Follower: %v, Term: %d}", p.Follower, p.Term)
}

func newVote(candidate string, term uint32, from string) *CmdMesg {
	return &CmdMesg{Data: Vote{Term: term, Candidate: candidate, From: from}}
}

func (v Vote) String() string {
	return fmt.Sprintf("Vote{Term: %d, Candidate: %v, From: %v}", v.Term, v.Candidate, v.From)
}

func newElection(candidate string, term uint32) *CmdMesg {
	return &CmdMesg{Data: Election{Term: term, Votes: 1, Candidate: candidate}}
}

func (e Election) Copy() *Election {
	return &Election{Term: e.Term, Votes: e.Votes, Candidate: e.Candidate}
}

func (e Election) String() string {
	return fmt.Sprintf("Election{Term: %d, Votes: %d, Candidate: %v}", e.Term, e.Votes, e.Candidate)
}

type Voter struct {
	name          string
	topic         string
	quorum        uint32
	maxleadertime int64
}

func NewVoter(id int, topic string, quorum uint32, maxleadertime int64) *Voter {
	return &Voter{
		name:          buildPeerName(id),
		topic:         topic,
		quorum:        quorum,
		maxleadertime: maxleadertime,
	}
}

func (v *Voter) startStateMachine(in <-chan Event, exit <-chan bool) <-chan Event {
	out := make(chan Event, 0)
	go func() {
		defer close(out)
		v.stateMachine(in, out, exit)
	}()
	return out
}

// stateMachine implements a RAFT election voter. It requires that the
// messages sent to the output channel have the same total-ordering
// for all voters participating in the elections.
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
//     The parmeter 'id' is a testing hook, to enable having
//     multiple voters in a single process running the test.
//
func (v *Voter) stateMachine(in <-chan Event, out chan<- Event, exit <-chan bool) {
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
		case <-exit:
			return
		case now := <-ticker.C:
			if now.Unix()-lasthearbeat > HeartTimeout {
				if state != Follower {
					log.Printf("grid: voter %v: transition: %v => %v", v.name, state, Follower)
				}
				state = Follower
				elect = nil
			}
			if state == Leader && (time.Now().Unix() < termstart+v.maxleadertime || v.maxleadertime == 0) {
				out <- NewWritable(v.topic, Key, newPing(v.name, elect.Term))
			}
			if time.Now().Unix() > nextelection && state == Follower {
				if state != Candidate {
					log.Printf("grid: voter %v: transition: %v => %v", v.name, state, Candidate)
				}
				state = Candidate
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				out <- NewWritable(v.topic, Key, newElection(v.name, term))
			}
		case event := <-in:
			var cmdmsg *CmdMesg

			switch msg := event.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				continue
			}

			switch data := cmdmsg.Data.(type) {
			case Pong:
			case Ping:
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				if v.name != data.Leader {
					state = Follower
				}
			case Vote:
				if elect == nil {
					continue
				}
				if elect.Candidate != data.From && elect.Candidate == data.Candidate && elect.Term == data.Term {
					elect.Votes = elect.Votes + 1
				}
				if elect.Votes >= v.quorum && elect.Candidate == v.name {
					if state != Leader {
						log.Printf("grid: voter %v: transition: %v => %v", v.name, state, Leader)
					}
					state = Leader
					termstart = time.Now().Unix()
				}
				if elect.Votes >= v.quorum && elect.Candidate != v.name {
					state = Follower
				}
				if elect.Votes >= v.quorum {
					lasthearbeat = time.Now().Unix()
					nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				}
			case Election:
				if elect != nil {
					continue
				}
				elect = data.Copy()
				term++
				if !voted[data.Term] && state != Leader {
					voted[data.Term] = true
					out <- NewWritable(v.topic, Key, newVote(data.Candidate, data.Term, v.name))
				}
			default:
				// Ignore other command messages.
			}
		}
	}
}
