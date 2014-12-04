package grid

import (
	"bytes"
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
	Name       string
	Health     Health
	LastPongTs int64
}

func newPeer(id string, r Rank, h Health, lastpong int64) *Peer {
	return &Peer{Name: id, Rank: r, Health: h, LastPongTs: lastpong}
}

type GridState struct {
	Term    uint32
	Version uint32
	Peers   map[string]*Peer
}

func (g *GridState) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("[term:%v, ver:%v, ", g.Term, g.Version))
	buf.WriteString("peers:[ \n")
	for _, peer := range g.Peers {
		buf.WriteString(fmt.Sprintf("   peer:[peerid:%s health:%d pongts:%d peerrank:%d] \n", peer.Name, peer.Health, peer.LastPongTs, peer.Rank))
	}
	buf.WriteString("]] ")
	return buf.String()
}

type Manager struct {
	term            uint32
	name            string
	rank            Rank
	gstate          *GridState
	in              <-chan Event
	out             chan<- Event
	exit            <-chan bool
	topic           string
	expectedPeerCnt int
	ops             map[string]*op
}

func NewManager(id int, topic string, expectedPeerCnt int) *Manager {
	name := buildName(id)

	gridState := &GridState{Term: 0, Peers: make(map[string]*Peer)}
	gridState.Peers[name] = newPeer(name, Follower, Active, time.Now().Unix())

	return &Manager{
		term:            0,
		name:            name,
		gstate:          gridState,
		topic:           topic,
		expectedPeerCnt: expectedPeerCnt,
	}
}

func (m *Manager) startStateMachine(in <-chan Event, exit <-chan bool) <-chan Event {
	out := make(chan Event, 0)
	go func() {
		defer close(out)
		m.in = in
		m.out = out
		m.exit = exit
		m.stateMachine()
	}()
	return out
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

			if now.Unix()-lasthearbeat > HeartTimeout {
				m.rank = Follower
			}

			if m.rank != Leader {
				continue
			}

			//We never mutate our state directly, it needs to be written into the append log, and read back
			//  just like everyone else.   Just in case we try to emit the log after we've lost an election

			m.gstate.Term = m.term

			gstateChanged := false
			activePeerCnt := 0

			for _, peer := range m.gstate.Peers { // Update health states
				if now.Unix()-peer.LastPongTs > HeartTimeout && peer.Health != Timeout {
					// Update peers that have timed out.
					log.Printf("grid: manager %v: peer[%v] transitioned from Health[%v -> %v]", m.name, peer.Name, peer.Health, Timeout)
					peer.Health = Timeout
					gstateChanged = true
					//TODO Take any actions we need to when a hosts first goes into Timeout state
				} else if now.Unix()-peer.LastPongTs > HeartTimeout {
					// This block will get call for timeout hosts who have been in that state more than one tick.
					// Do we need this for correctness?
				} else if now.Unix()-peer.LastPongTs <= HeartTimeout && peer.Health == Timeout {
					// Update peer that are now active.
					log.Printf("grid: manager %v: peer[%v] transitioned from Health[%v -> %v]", m.name, peer.Name, peer.Health, Active)
					peer.Health = Active
					gstateChanged = true
					activePeerCnt++
				} else if peer.Health == Active {
					// Look for active peers.
					activePeerCnt++
				}

				// Demote anyone who isn't me.
				if peer.Rank == Leader && peer.Name != m.name {
					peer.Rank = Follower
					gstateChanged = true
				} else if peer.Rank != Leader && peer.Name == m.name {
					m.gstate.Peers[m.name].Rank = Leader
					gstateChanged = true
				}
			}

			if gstateChanged && activePeerCnt == m.expectedPeerCnt {
				m.gstate.Version++
				log.Printf("grid: manager %v: emitting new state; \ngstate:%s ", m.name, m.gstate)
				m.out <- NewWritable(m.topic, Key, &CmdMesg{Data: *m.gstate})
			}

		case event := <-m.in:
			var cmdmsg *CmdMesg

			// Extract command message.
			switch msg := event.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				continue
			}

			// Check for type of message.
			switch data := cmdmsg.Data.(type) {
			case Ping:
				lasthearbeat = time.Now().Unix()
				// log.Printf("ping:[%v] rank:%v cterm:%v myname:%v ", data, m.rank, m.term, m.name)
				if data.Term < m.term {
					log.Printf("gird: manager %v: ping: term mismatch", m.name)
					continue
				}
				m.term = data.Term

				if data.Leader == m.name {
					log.Printf("grid: manager %v: ping: I'm a leader; term=%d", m.name, m.term)
					m.rank = Leader
				} else {
					log.Printf("grid: manager %v: ping: I'm a follower; term=%d", m.name, m.term)
					m.gstate.Peers[m.name].Rank = Follower
					m.rank = Follower
					m.out <- NewWritable(m.topic, Key, newPong(m.name, data.Term))
				}
			case Pong:
				// log.Printf("pong:[%v] rank:%v cterm:%v", data, m.rank, m.term)
				if data.Term < m.term {
					continue
				}
				if _, ok := m.gstate.Peers[data.Follower]; !ok && m.rank == Leader {
					// Only the leader can add new peers.
					m.gstate.Peers[data.Follower] = newPeer(data.Follower, Follower, Active, time.Now().Unix())
				}
			case GridState:
				// log.Printf("gridstate: name:%v rank:%v cterm:%v newgstate[%v] currgstate:[%v]  ", m.name, m.rank, m.term, data.String(), oldGstate.String())
				//TODO For now I'm going to allow duplicate versions to be re-processed,
				// otherwise the leader would reject the new state.  I think I need to rework the code so that the code above works on a copy until its received
				if data.Version < m.gstate.Version {
					log.Printf("warning: grid: manager %v: gstate: got a new gstate with an old version; oldgs:%v \nnewgs:%v ", m.name, m.gstate, data)
					continue
				}
				if data.Term < m.term {
					log.Printf("warning: grid: manager %v: gstate: got a new gstate with an old term; oldgs:%v \nnewgs:%v ", m.name, m.gstate, data)
					continue
				}
				m.gstate = &data
				// Act on my part of the state, do Func schedule for m.gstate.Peers[m.name].
			default:
				// Ignore other command messages.
			}
		}
	}
}

func buildName(id int) string {
	host, err := os.Hostname()
	if err != nil {
		log.Printf("grid: failed to aquire the peer's hostname: %v", err)
	}

	return fmt.Sprintf("%v-%v-%v", host, os.Getpid(), id)
}
