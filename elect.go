package grid

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	Skew         = 20
	TickMillis   = 500
	HeartTimeout = 2
	ElectTimeout = 10
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

// VoterMesg is an envelope for more specific voting messages.
type VoterMesg struct {
	From  int
	State int
	Data  interface{}
}

func NewPing(leader int) *Ping {
	return &Ping{Leader: leader}
}

func (p *Ping) String() string {
	return fmt.Sprintf("Ping{Leader: %d}", p.Leader)
}

func NewVote(candidate int, term uint32, from int) *Vote {
	return &Vote{Term: term, Candidate: candidate, From: from}
}

func (v *Vote) String() string {
	return fmt.Sprintf("Vote{Term: %d, Candidate: %d, From: %d}", v.Term, v.Candidate, v.From)
}

func NewElection(candidate int, term uint32) *Election {
	return &Election{Term: term, Votes: 1, Candidate: candidate}
}

func (e Election) Copy() *Election {
	return &Election{Term: e.Term, Votes: e.Votes, Candidate: e.Candidate}
}

func (e *Election) String() string {
	return fmt.Sprintf("Election{Term: %d, Votes: %d, Candidate: %d}", e.Term, e.Votes, e.Candidate)
}

func NewVoterMesg(name int, state int, data interface{}) *VoterMesg {
	return &VoterMesg{From: name, State: state, Data: data}
}

func (m *VoterMesg) String() string {
	return fmt.Sprintf("VoterMesg{From: %d, State: %d, Data: %v}", m.From, m.State, m.Data)
}

// Voter implements a RAFT election voter. It requires that the messages
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
//     3. Upon receiving an Election, vote, but never for
//        yourself.
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
func Voter(name int, quorum uint32, maxleadertime int64, in <-chan *VoterMesg, out chan<- *VoterMesg) {
	defer close(out)

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
				state = Follower
				elect = nil
			}
			if state == Leader {
				if time.Now().Unix() < termstart+maxleadertime && maxleadertime > 0 {
					out <- &VoterMesg{From: name, State: state, Data: NewPing(name)}
				}
			}
			if time.Now().Unix() > nextelection && state == Follower {
				state = Candidate
				voted[term] = true
				nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				out <- &VoterMesg{From: name, State: state, Data: NewElection(name, term)}
			}
		case m := <-in:
			switch data := m.Data.(type) {
			case *Ping:
				fmt.Printf("%v rx: %v\n", name, data)
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + ElectTimeout + rng.Int63n(Skew)
				if name != data.Leader {
					state = Follower
				}
			case *Vote:
				fmt.Printf("%v rx: %v\n", name, data)
				if elect == nil {
					continue
				}
				if elect.Candidate != data.From && elect.Candidate == data.Candidate && elect.Term == data.Term {
					elect.Votes = elect.Votes + 1
				}
				if elect.Votes >= quorum && elect.Candidate == name {
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
			case *Election:
				fmt.Printf("%v rx: %v\n", name, data)
				if elect != nil {
					continue
				}
				fmt.Printf("%v accepting election: %v\n", name, data)
				elect = data.Copy()
				term++
				if !voted[data.Term] && state != Leader {
					voted[data.Term] = true
					out <- &VoterMesg{From: name, State: state, Data: NewVote(data.Candidate, data.Term, name)}
				}
			default:
				// Ignore anything not related to the election.
			}
		}
	}
}
