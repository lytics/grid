package grid

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
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

type ChangeType int

const (
	MemberHealth ChangeType = iota
	Leadership
	FuncSch
)

type GridStateChange struct {
	ChangeType ChangeType
	PeerId     string
	Gstate     *GridState
}

type FuncSchPlaceHolder struct {
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
	lock               *sync.Mutex
}

func NewManager(id int, topic string, quorum uint32, maxleadertime int64, in <-chan Event, out chan<- Event, exit <-chan bool) *Manager {
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
		lock:               &sync.Mutex{}, //We may not need this lock, if the statemachine is the only one doing the mutations.
	}
}

func (m *Manager) CloneGstate() *GridState {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.gstate.DeepCopy()
}

func (m *Manager) SwapGstate(gs *GridState) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.gstate.Version != gs.Version-1 {
		log.Printf("Warning: Swapping gstate: The new version greater than current version plus one. oldgs:%v newgs:%v ", m.gstate, gs)
	}
	m.gstate = gs
}

func (m *Manager) SetFuncSch(peerId string, funcSch *FuncSchPlaceHolder) error {

	if m.rank != Leader {
		return errors.New(fmt.Sprintf("trying to mutate grid state, without being the grid Leader. rank[%v] term[%v]", m.rank, m.term))
	}

	//TODO add the funcSch to m.gstate.Peers[peerId].FuncSch

	m.gstate.Version++
	m.out <- NewWritable(m.topic, Key, m.gstate)
	m.lastEmittedVersion = m.gstate.Version

	return nil
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
			tmpgstate := m.CloneGstate()

			tmpgstate.Term = m.term

			gstateChanged := false

			for _, peer := range tmpgstate.Peers { // Update health states
				if now.Unix()-peer.LastPongTs > m.HeartTimeout && peer.Health != Timeout {
					//Look for peers who have timed out.
					oldHealth := peer.Health
					peer.Health = Timeout
					gstateChanged = true
					log.Printf("statemachine:peer[%v] transitioned from Health[%v -> %v]", peer.PeerId, oldHealth, peer.Health)
					//TODO Take any actions we need to when a hosts first goes into Timeout state
				} else if now.Unix()-peer.LastPongTs > m.HeartTimeout {
					//TODO this block will get call for timeout hosts who have been in that state more than one tick...
					//TODO how long we we leave them in a timeout state before taking action?
				} else if now.Unix()-peer.LastPongTs <= m.HeartTimeout && peer.Health == Timeout {
					//Look for peers who have returned to a healthy state
					oldHealth := peer.Health
					peer.Health = Active
					gstateChanged = true
					log.Printf("statemachine:peer[%v] transitioned from Health[%v -> %v]", peer.PeerId, oldHealth, peer.Health)
				}

				//Demote any who aren't me
				if peer.Rank == Leader && peer.PeerId != m.name {
					peer.Rank = Follower
					gstateChanged = true
				} else if peer.Rank != Leader && peer.PeerId == m.name {
					tmpgstate.Peers[m.name].Rank = Leader
					gstateChanged = true
				}
			}

			if gstateChanged {
				tmpgstate.Version++
			}

			if m.lastEmittedVersion < tmpgstate.Version {
				m.out <- NewWritable(m.topic, Key, tmpgstate)
				m.lastEmittedVersion = tmpgstate.Version
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
				log.Printf("ping:[%v] rank:%v cterm:%v", data, m.rank, m.term)
				if data.Term < m.term {
					continue
				}
				m.term = data.Term

				if data.Leader == m.name {
					m.rank = Leader
				} else {
					m.gstate.Peers[m.name].Rank = Follower
					m.rank = Follower
				}

			case Pong:

				log.Printf("pong:[%v] rank:%v cterm:%v", data, m.rank, m.term)
				if data.Term != m.term { // not sure if the should be < instead of !=
					continue
				}

				if peer, ok := m.gstate.Peers[data.Follower]; !ok && m.rank == Leader {
					//Only the leader can add new peers
					peer := &Peer{Rank: Follower, PeerId: data.Follower, Health: Active, LastPongTs: time.Now().Unix()}
					m.gstate.Peers[data.Follower] = peer
					m.gstate.Version++
					log.Printf("pong: new peer added to the grid: %v", peer)

				} else {
					//always update the last pong time, even if your a follower.
					//  that way if we take over leadership we'll have a good idea of which peers are still active.
					peer.LastPongTs = time.Now().Unix()
				}

			case GridState:
				oldGstate := m.gstate

				log.Printf("gridstate: newgstate[%v] currgstate:[%v] rank:%v cterm:%v ", data, oldGstate, m.rank, m.term)
				if data.Version <= oldGstate.Version {
					log.Printf("Warning: gstate: Got a new gstate with an old version. oldgs:%v newgs:%v ", oldGstate, data)
					continue
				}

				if data.Term <= m.term {
					log.Printf("Warning: gstate: Got a new gstate from an old election term. oldgs:%v newgs:%v ", oldGstate, data)
					continue
				}

				m.gstate = &data

				// detect changes....

				//TODO add a case for GridState and not the leader.   That is this is were the
				// followers will get notifications for changes.
				//    TODO if data.Version <= GridStateVersion, then ignore this message.
				//    TODO store the state's version in our gridStateVersion
				//    TODO Detect if the Peers list changed, but only log it.
				//    TODO Detect if the FunctionState changed for my id

				//Do leader logic below this point.
				if m.rank != Leader { // not sure if the should be < instead of !=
					continue
				}
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
