package grid

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type Health int

const (
	Active Health = iota
	Timeout
)

type Peer struct {
	Rank       Rank
	PeerId     string
	Health     Health
	LastPongTs int64
}

func (p *Peer) DeepCopy() *Peer {
	tmpp := &Peer{}
	tmpp.Rank = p.Rank
	tmpp.PeerId = p.PeerId
	tmpp.Health = p.Health
	tmpp.LastPongTs = p.LastPongTs
	return tmpp
}

type GridState struct {
	Term    uint32
	Version uint32
	Peers   map[string]*Peer
}

func (g *GridState) DeepCopy() *GridState {
	tmpgs := &GridState{}

	tmpgs.Term = g.Term
	tmpgs.Version = g.Version
	tmpgs.Peers = make(map[string]*Peer)
	for _, peer := range g.Peers {
		tmppeer := peer.DeepCopy()
		tmpgs.Peers[tmppeer.PeerId] = tmppeer
	}

	return tmpgs
}

func (g *GridState) String() string {
	b, err := json.Marshal(g)
	if err != nil {
		return ""
	}
	return string(b)
}

type Manager struct {
	term               uint32
	name               string
	rank               Rank
	gstate             *GridState
	in                 <-chan Event
	out                chan<- Event
	exit               <-chan bool
	id                 int
	topic              string
	HeartTimeout       int64 //override-able
	lastEmittedVersion uint32
	expectedPeerCnt    int
}

func NewManager(id int, topic string, expectedPeerCnt int, in <-chan Event, out chan<- Event, exit <-chan bool) *Manager {
	name := buildPeerId(id)
	gridState := &GridState{Term: 0, Peers: make(map[string]*Peer)}
	gridState.Peers[name] = &Peer{Rank: Follower, PeerId: name, Health: Active, LastPongTs: time.Now().Unix()}
	return &Manager{
		term:               0,
		name:               name,
		rank:               Follower,
		gstate:             gridState,
		in:                 in,
		out:                out,
		exit:               exit,
		id:                 id,
		topic:              topic,
		HeartTimeout:       HeartTimeout,
		lastEmittedVersion: 0,
		expectedPeerCnt:    expectedPeerCnt,
	}
}

func (m *Manager) startStateMachine() {
	go func() {
		//TODO maybe add logic to prevent starting this go routine twice???
		m.stateMachine()
	}()
}

func (m *Manager) stateMachine() {
	ticker := time.NewTicker(TickMillis * time.Millisecond)
	defer ticker.Stop()

	lasthearbeat := time.Now().Unix()

	for {
		select {
		case <-m.exit:
			return
		case now := <-ticker.C:

			if now.Unix()-lasthearbeat > m.HeartTimeout {
				m.rank = Follower
			}

			if m.rank != Leader { // not sure if the should be < instead of !=
				continue
			}

			//We never mutate our state directly, it needs to be written into the append log, and read back
			//  just like everyone else.   Just in case we try to emit the log after we've lost an election

			m.gstate.Term = m.term

			gstateChanged := false
			activePeerCnt := 0
			timedOuthosts := 0

			for _, peer := range m.gstate.Peers { // Update health states
				if now.Unix()-peer.LastPongTs > m.HeartTimeout && peer.Health != Timeout {
					//Look for peers who have gone from active to timeout.
					oldHealth := peer.Health
					peer.Health = Timeout
					gstateChanged = true
					timedOuthosts++
					log.Printf("statemachine:peer[%v] transitioned from Health[%v -> %v]", peer.PeerId, oldHealth, peer.Health)
					//TODO Take any actions we need to when a hosts first goes into Timeout state
				} else if now.Unix()-peer.LastPongTs > m.HeartTimeout {
					//this block will get call for timeout hosts who have been in that state more than one tick...
				} else if now.Unix()-peer.LastPongTs <= m.HeartTimeout && peer.Health == Timeout {
					//Look for peers who have are going from timeout to active
					oldHealth := peer.Health
					peer.Health = Active
					gstateChanged = true
					activePeerCnt++
					log.Printf("statemachine:peer[%v] transitioned from Health[%v -> %v]", peer.PeerId, oldHealth, peer.Health)
				} else if peer.Health == Active {
					//Look for active peers
					activePeerCnt++
				}

				//Demote any who isn't me
				if peer.Rank == Leader && peer.PeerId != m.name {
					peer.Rank = Follower
					gstateChanged = true
				} else if peer.Rank != Leader && peer.PeerId == m.name {
					m.gstate.Peers[m.name].Rank = Leader
					gstateChanged = true
				}
			}

			if gstateChanged && activePeerCnt == m.expectedPeerCnt {
				m.gstate.Version++
				log.Printf("statemachine: emitting new state. leader-name:%s \ngstate:%s ", m.name, m.gstate.String())

				//TODO generate the func schedule.

				m.out <- NewWritable(m.topic, Key, &CmdMesg{Data: *m.gstate})
				m.lastEmittedVersion = m.gstate.Version
			} else if timedOuthosts > 0 && activePeerCnt+timedOuthosts == m.expectedPeerCnt {
				m.gstate.Version++
				log.Printf("statemachine: We have lost hosts in the grid, emitting new state. leader-name:%s \ngstate:%s ", m.name, m.gstate.String())

				//TODO generate the func schedule.  //For now do we just shut down all work???

				m.out <- NewWritable(m.topic, Key, &CmdMesg{Data: *m.gstate})
				m.lastEmittedVersion = m.gstate.Version
			}

		case inmsg := <-m.in:
			var cmdmsg *CmdMesg

			// Extract command message.
			switch msg := inmsg.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				continue
			}

			// Check for type of message.
			switch data := cmdmsg.Data.(type) {
			case Ping:
				lasthearbeat = time.Now().Unix()
				//log.Printf("ping:[%v] rank:%v cterm:%v myname:%v ", data, m.rank, m.term, m.name)
				if data.Term < m.term {
					log.Printf("ping[%v]: term mismatch. rank:%v cterm:%v myname:%v ", data, m.rank, m.term, m.name)
					continue
				}
				m.term = data.Term

				if data.Leader == m.name {
					//log.Printf("ping: Im a leader. term=%d", m.term)
					m.rank = Leader
				} else {
					//log.Printf("ping: Im a follower. term=%d", m.term)
					m.gstate.Peers[m.name].Rank = Follower
					m.rank = Follower
					m.out <- NewWritable(m.topic, Key, newPong(m.name, data.Term))
				}

			case Pong:
				//log.Printf("pong:[%v] rank:%v cterm:%v", data, m.rank, m.term)
				if data.Term != m.term { // not sure if the should be < instead of !=
					continue
				}

				if _, ok := m.gstate.Peers[data.Follower]; !ok && m.rank == Leader {
					//Only the leader can add new peers
					peer := &Peer{Rank: Follower, PeerId: data.Follower, Health: Active, LastPongTs: time.Now().Unix()}
					m.gstate.Peers[data.Follower] = peer
				}
			case GridState:
				oldGstate := m.gstate
				//log.Printf("gridstate: name:%v rank:%v cterm:%v newgstate[%v] currgstate:[%v]  ", m.name, m.rank, m.term, data.String(), oldGstate.String())
				if data.Version < m.gstate.Version {
					//TODO For now I'm going to allow duplicate versions to be re-processed,
					// otherwise the leader would reject the new state.  I think I need to rework the code so that the code above works on a copy until its received

					log.Printf("Warning: gstate: Got a new gstate with an old or matching version. name:%v oldgs:%v \nnewgs:%v ", m.name, oldGstate.String(), data.String())
					continue
				}

				if data.Term < m.term {
					log.Printf("Warning: gstate: Got a new gstate from an old election term. name:%v oldgs:%v \nnewgs:%v ", m.name, oldGstate.String(), data.String())
					continue
				}

				m.gstate = &data

				//act on my part of the state
				// Do Func schedule for m.gstate.Peers[m.name]

			default:
				log.Printf("Unknown message: msg:%v term:%v rank:%v name:%v", cmdmsg, m.term, m.rank, m.name)
			}
		}
	}
}

func buildPeerId(id int) string {
	host, err := os.Hostname()
	if err != nil {
		log.Printf("grid: failed to aquire the peer's hostname: %v", err)
	}

	return fmt.Sprintf("%v-%v-%v", host, os.Getpid(), id)
}
