package grid

import (
	"log"
	"math/rand"
	"time"
)

type Voter struct {
	name  string
	epoch uint64
	opts  *CoordOptions
	*Grid
}

func NewVoter(id int, opts *CoordOptions, g *Grid) *Voter {
	return &Voter{buildPeerName(id), 0, opts, g}
}

func (v *Voter) startStateMachine(in <-chan Event) <-chan Event {
	out := make(chan Event, 0)
	go func() {
		defer close(out)
		v.stateMachine(in, out)
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
//     The parameter 'id' is a testing hook, to enable having
//     multiple voters in a single process running the test.
//
func (v *Voter) stateMachine(in <-chan Event, out chan<- Event) {
	ticker := time.NewTicker(v.opts.TickDuration())
	defer ticker.Stop()

	voted := make(map[uint32]bool)
	rng := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	state := Follower

	lasthearbeat := time.Now().Unix()
	nextelection := time.Now().Unix() + v.opts.ElectTimeout + rng.Int63n(v.opts.Skew)

	var termstart int64
	var elect *Election
	var term uint32
	for {
		select {
		case <-v.exit:
			return
		case now := <-ticker.C:
			if now.Unix()-lasthearbeat > v.opts.HeartTimeout {
				if state != Follower {
					log.Printf("grid: voter %v: transition: %v => %v", v.name, state, Follower)
				}
				state = Follower
				elect = nil
			}
			if state == Leader && (time.Now().Unix() < termstart+v.maxleadertime || v.maxleadertime == 0) {
				out <- NewWritable(v.cmdtopic, nil, newPing(v.epoch, v.name, elect.Term))
			}
			if time.Now().Unix() > nextelection && state == Follower {
				if state != Candidate {
					log.Printf("grid: voter %v: transition: %v => %v", v.name, state, Candidate)
				}
				state = Candidate
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + v.opts.ElectTimeout + rng.Int63n(v.opts.Skew)
				out <- NewWritable(v.cmdtopic, nil, newElection(v.epoch, v.name, term))
			}
		case event, open := <-in:
			if !open {
				return
			}

			var cmdmsg *CmdMesg

			switch msg := event.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				continue
			}

			if cmdmsg.Epoch != v.epoch {
				log.Printf("warning: grid: voter %v: command message epoch mismatch %v != %v", v.name, cmdmsg.Epoch, v.epoch)
				continue
			}

			switch data := cmdmsg.Data.(type) {
			case Ping:
				lasthearbeat = time.Now().Unix()
				nextelection = time.Now().Unix() + v.opts.ElectTimeout + rng.Int63n(v.opts.Skew)
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
					nextelection = time.Now().Unix() + v.opts.ElectTimeout + rng.Int63n(v.opts.Skew)
				}
			case Election:
				if elect != nil {
					continue
				}
				elect = data.Copy()
				term++
				if !voted[data.Term] && state != Leader {
					voted[data.Term] = true
					out <- NewWritable(v.cmdtopic, nil, newVote(v.epoch, data.Candidate, data.Term, v.name))
				}
			case PeerState:
				if data.Epoch != 0 {
					v.epoch = data.Epoch
				}
			default:
				// Ignore other command messages.
			}
		}
	}
}
