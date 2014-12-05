package grid

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Manager struct {
	name  string
	state *PeerState
	*Grid
}

func NewManager(id int, g *Grid) *Manager {
	return &Manager{buildPeerName(id), newPeerState(), g}
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
	ticker := time.NewTicker(TickMillis * time.Millisecond)
	defer ticker.Stop()

	lasthearbeat := time.Now().Unix()
	m.state.Peers[m.name] = newPeer(m.name, Follower, Active, time.Now().Unix())

	var rank Rank
	var term uint32
	for {
		select {
		case <-m.exit:
			return
		case now := <-ticker.C:

			if now.Unix()-lasthearbeat > HeartTimeout {
				rank = Follower
			}

			if rank != Leader {
				continue
			}

			m.state.Term = term

			changed := false
			activepeers := 0
			inactivepeers := 0

			for _, peer := range m.state.Peers { // Update health states
				if now.Unix()-peer.LastPongTs > HeartTimeout && peer.Health != Inactive {
					// Update peers that have timed out.
					log.Printf("grid: manager %v: peer[%v] transitioned from Health[%v -> %v]", m.name, peer.Name, peer.Health, Inactive)
					peer.Health = Inactive
					changed = true
					inactivepeers++
				} else if now.Unix()-peer.LastPongTs > HeartTimeout {
					// Don't log over and over that a peer is timed out.
				} else if now.Unix()-peer.LastPongTs <= HeartTimeout && peer.Health == Inactive {
					// Update peer that are now active.
					log.Printf("grid: manager %v: peer[%v] transitioned from Health[%v -> %v]", m.name, peer.Name, peer.Health, Active)
					peer.Health = Active
					changed = true
					activepeers++
				} else if peer.Health == Active {
					// Look for active peers.
					activepeers++
				}

				// Demote any one who isn't me
				if peer.Rank == Leader && peer.Name != m.name {
					peer.Rank = Follower
					changed = true
				} else if peer.Rank != Leader && peer.Name == m.name {
					m.state.Peers[m.name].Rank = Leader
					changed = true
				}
			}

			if changed && activepeers == m.npeers {
				m.state.Version++
				m.state.Sched = peersched(m.state.Peers, m.ops, m.parts)
				log.Printf("grid: manager %v: emitting new start state; \ngstate:%s ", m.name, m.state)
				out <- NewWritable(m.cmdtopic, Key, &CmdMesg{Data: *m.state})
			} else if inactivepeers > 0 && activepeers+inactivepeers == m.npeers {
				m.state.Version++
				// Everyone should be told to stop, emit state as such.
				log.Printf("grid: manager %v: emitting new stop state; \ngstate:%s ", m.name, m.state)
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
					log.Printf("gird: manager %v: ping: term mismatch: rank: %v cterm: %v", m.name, rank, term)
					continue
				}
				term = data.Term
				if data.Leader == m.name {
					rank = Leader
					m.state.Peers[m.name].LastPongTs = time.Now().Unix()
				} else {
					m.state.Peers[m.name].Rank = Follower
					rank = Follower
					out <- NewWritable(m.cmdtopic, Key, newPong(m.name, data.Term))
				}
			case Pong:
				// log.Printf("pong:[%v] rank:%v cterm:%v", data, m.rank, m.term)
				if data.Term < term {
					continue
				}
				m.state.Peers[data.Follower] = newPeer(data.Follower, Follower, Active, time.Now().Unix())
			case PeerState:
				log.Printf("grid: manager %v rank:%v cterm:%v newgstate[%v] currgstate:[%v]  ", m.name, rank, term, data, m.state)
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
