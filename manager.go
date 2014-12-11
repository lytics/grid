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
	state       *PeerState
	tkohandler  func() // Test hook.
	*Grid
}

func NewManager(id int, g *Grid) *Manager {
	name := buildPeerName(id)
	tkohandler := func() {
		log.Fatalf("grid: manager %v: exiting due to one or more peers going unhealthy, the grid needs to be restarted.", name)
	}
	return &Manager{name, PeerTimeout, newPeerState(), tkohandler, g}
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
	log.Printf("grid: manager %v: starting: number of peers: %v", m.name, m.npeers)
	ticker := time.NewTicker(TickMillis * time.Millisecond)
	defer ticker.Stop()

	lasthearbeat := time.Now().Unix()
	m.state.Peers[m.name] = newPeer(m.name, time.Now().Unix())

	var rank Rank
	var term uint32
	var stateclosed bool

	for {
		select {
		case <-m.exit:
			return
		case now := <-ticker.C:

			if now.Unix()-lasthearbeat > HeartTimeout {
				rank = Follower
			}

			m.state.Term = term

			for _, peer := range m.state.Peers {
				if now.Unix()-peer.LastPongTs > m.peertimeout && len(m.state.Peers) >= m.npeers {
					// Update peers that have timed out. This should only happen if the peer became unhealthy.
					log.Printf("grid: manager %v: Peer[%v] transitioned from Health[Active -> Inactive]", m.name, peer.Name)
					m.tkohandler()
					return
				} else if len(m.state.Peers) >= m.npeers && !stateclosed {
					if rank != Leader {
						continue
					}
					// We've reached the required number of peers for the first time, note that
					// we only ever emit once because of the guard.
					stateclosed = true
					m.state.Version++
					m.state.Sched = peersched(m.state.Peers, m.ops, m.parts)
					log.Printf("grid: manager %v: emitting start state v%d", m.name, m.state.Version)
					out <- NewWritable(m.cmdtopic, Key, &CmdMesg{Data: *m.state})
				}
			}
		case event := <-in:
			var cmdmsg *CmdMesg

			// Extract command message.
			switch msg := event.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				log.Printf("warning: grid: manager %v: message type %T didn't match the expected type %T", m.name, msg, &CmdMesg{})
				continue
			}

			// Check for type of message.
			switch data := cmdmsg.Data.(type) {
			case Ping:
				lasthearbeat = time.Now().Unix()
				if data.Term < term {
					log.Printf("warning: gird: manager %v: received leader ping term mismatch: current: %d: new term: %d", m.name, term, data.Term)
					continue
				}
				term = data.Term
				if data.Leader == m.name {
					rank = Leader
					m.state.Peers[m.name].LastPongTs = time.Now().Unix()
				} else {
					rank = Follower
					out <- NewWritable(m.cmdtopic, Key, newPong(m.name, data.Term))
				}
				m.state.Peers[data.Leader] = newPeer(data.Leader, time.Now().Unix())
			case Pong:
				if data.Term < term {
					continue
				}
				if _, ok := m.state.Peers[data.Follower]; !ok {
					log.Printf("grid: manager %v: Peer[%v] transitioned from Health[Inactive -> Active]", m.name, data.Follower)
				}
				m.state.Peers[data.Follower] = newPeer(data.Follower, time.Now().Unix())
			case PeerState:
				if data.Version < m.state.Version {
					log.Printf("warning: grid: manager %v: received a new state with an old version: current: %d: new: %d", m.name, m.state.Version, data.Version)
					continue
				}
				if data.Term < term {
					log.Printf("warning: grid: manager %v: received a new state with an old term: current: %d: new: %d", m.name, m.state.Term, data.Term)
					continue
				}
				m.state = &data
				for _, instance := range m.state.Sched[m.name] {
					go m.startinst(instance)
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
		log.Fatalf("fatal: grid: failed to aquire hostname: %v", err)
	}

	return fmt.Sprintf("%v-%v-%v", host, os.Getpid(), id)
}
