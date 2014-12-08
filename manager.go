package grid

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Manager struct {
	name        string
	peertimeout int64
	tkohander   func()
	state       *PeerState
	*Grid
}

func NewManager(id int, g *Grid) *Manager {
	name := buildPeerName(id)
	defualttkohandler := func() {
		log.Fatalf("grid: manager %v: Exiting due to one or more peers going unhealthy, the grid needs to be restarted.", name)
	}
	return &Manager{name, PeerTimeout, defualttkohandler, newPeerState(), g}
}

func (m *Manager) startStateMachine(in <-chan Event) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		m.stateMachine(in, out)
	}()
	return out
}

func (m *Manager) stateMachine(in <-chan Event, out chan<- Event) {
	log.Printf("grid: manager %v: starting: npeers:%v", m.name, m.npeers)
	ticker := time.NewTicker(TickMillis * time.Millisecond)
	defer ticker.Stop()

	lasthearbeat := time.Now().Unix()
	m.state.Peers[m.name] = newPeer(m.name, time.Now().Unix())

	var rank Rank
	var leader string
	var term uint32
	var ready bool = false

	for {
		select {
		case <-m.exit:
			return
		case now := <-ticker.C:

			if now.Unix()-lasthearbeat > HeartTimeout {
				rank = Follower
			}

			m.state.Term = term
			emit := false

			for _, peer := range m.state.Peers {
				if now.Unix()-peer.LastPongTs > m.peertimeout && len(m.state.Peers) >= m.npeers {
					// Update peers that have timed out.  This should only happen if the peer was happen and then became unhealthy.
					log.Printf("grid: manager %v: peer[%v] transitioned from Health[Active -> Inactive]", m.name, peer.Name)
					m.tkohander()
					return
				} else if len(m.state.Peers) >= m.npeers && !ready {
					//We've reached the required number of peers for the first time
					ready = true
					emit = true
				}
			}

			if rank != Leader {
				continue
			}

			if emit {
				emit = false
				m.state.Version++
				m.state.Sched = peersched(m.state.Peers, m.ops, m.parts)
				log.Printf("grid: manager %v: emitting new start state; \ngstate:%s ", m.name, m.state)
				out <- NewWritable(m.cmdtopic, Key, &CmdMesg{Data: *m.state})
			}

		case event := <-in:
			var cmdmsg *CmdMesg

			// Extract command message.
			switch msg := event.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				log.Printf("The message type %T didn't match the expected type of %T", msg, &CmdMesg{})
				continue
			}

			// Check for type of message.
			switch data := cmdmsg.Data.(type) {
			case Ping:
				lasthearbeat = time.Now().Unix()
				// log.Printf("ping:[%v] rank:%v cterm:%v myname:%v ", data, m.rank, m.term, m.name)
				if data.Term < term {
					log.Printf("gird: manager %v: ping: term mismatch: rank: %v cterm: %v leader: %v", m.name, rank, term, leader)
					continue
				}
				term = data.Term
				if data.Leader == m.name {
					rank = Leader
					leader = m.name
					m.state.Peers[m.name].LastPongTs = time.Now().Unix()
				} else {
					rank = Follower
					leader = data.Leader
					out <- NewWritable(m.cmdtopic, Key, newPong(m.name, data.Term))
				}
				m.state.Peers[data.Leader] = newPeer(data.Leader, time.Now().Unix())
			case Pong:
				// log.Printf("pong:[%v] rank:%v cterm:%v", data, m.rank, m.term)
				if data.Term < term {
					continue
				}
				if _, ok := m.state.Peers[data.Follower]; !ok {
					log.Printf("grid: manager %v: peer[%v] transitioned from Health[Undiscovered -> Active]", m.name, data.Follower)
				}
				m.state.Peers[data.Follower] = newPeer(data.Follower, time.Now().Unix())
			case PeerState:
				log.Printf("grid: manager %v rank:%v leader:%v cterm:%v newgstate[%v] currgstate:[%v]  ", m.name, rank, leader, term, data, m.state)
				if data.Version < m.state.Version {
					log.Printf("warning: grid: manager %v: gstate: got a new gstate with an old version; oldgs:%v \nnewgs:%v ", m.name, m.state, data)
					continue
				}
				if data.Term < term {
					log.Printf("warning: grid: manager %v: gstate: got a new gstate with an old term; oldgs:%v \nnewgs:%v ", m.name, m.state, data)
					continue
				}
				m.state = &data
				for _, instance := range m.state.Sched[m.name] {
					m.starti(instance)
				}
			default:
				// Ignore other command messages.
			}
		}
	}
}

func buildPeerName(id int) string {
	host, err := os.Hostname()
	if err != nil {
		log.Printf("grid: failed to aquire the peer's hostname: %v", err)
	}

	return fmt.Sprintf("%v-%v-%v", host, os.Getpid(), id)
}
