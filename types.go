package grid

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
)

// CmdMesg is an envelope for more specific messages on the command topic.
type CmdMesg struct {
	Data interface{}
}

func newCmdMesg(data interface{}) *CmdMesg {
	return &CmdMesg{Data: data}
}

func (m *CmdMesg) String() string {
	return fmt.Sprintf("CmdMesg{Data: %v}", m.Data)
}

type coder struct {
	*gob.Encoder
	*gob.Decoder
}

func (c *coder) New() interface{} {
	return &CmdMesg{}
}

func NewCmdMesgDecoder(r io.Reader) Decoder {
	return &coder{nil, gob.NewDecoder(r)}
}

func NewCmdMesgEncoder(w io.Writer) Encoder {
	return &coder{gob.NewEncoder(w), nil}
}

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

func newPing(leader string, term uint32) *CmdMesg {
	return &CmdMesg{Data: Ping{Leader: leader, Term: term}}
}

func (p Ping) String() string {
	return fmt.Sprintf("Ping{Leader: %v, Term: %d}", p.Leader, p.Term)
}

// Pong is send only by followers in response to a leader's Ping.
type Pong struct {
	Follower string
	Term     uint32
}

func newPong(follower string, term uint32) *CmdMesg {
	return &CmdMesg{Data: Pong{Follower: follower, Term: term}}
}

func (p Pong) String() string {
	return fmt.Sprintf("Pong{Follower: %v, Term: %d}", p.Follower, p.Term)
}

// Vote is sent by any voter in response to an election.
type Vote struct {
	Term      uint32
	Candidate string
	From      string
}

func newVote(candidate string, term uint32, from string) *CmdMesg {
	return &CmdMesg{Data: Vote{Term: term, Candidate: candidate, From: from}}
}

func (v Vote) String() string {
	return fmt.Sprintf("Vote{Term: %d, Candidate: %v, From: %v}", v.Term, v.Candidate, v.From)
}

// Election is started by any voter that has timed out the leader.
type Election struct {
	Term      uint32
	Votes     uint32
	Candidate string
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

func newPeer(name string, r Rank, h Health, lastpong int64) *Peer {
	return &Peer{Name: name, Rank: r, Health: h, LastPongTs: lastpong}
}

type PeerState struct {
	Term    uint32
	Version uint32
	Sched   PeerSched
	Peers   map[string]*Peer
}

func (ps *PeerState) String() string {
	b, err := json.Marshal(ps)
	if err != nil {
		return ""
	}
	return string(b)
}

func newPeerState() *PeerState {
	return &PeerState{Term: 0, Peers: make(map[string]*Peer)}
}

// Instance is the full mapping of topic slices that a particular
// running instance of 'f' will read from, since a 'f' could read
// from multiple topics.
type Instance struct {
	i           int
	fname       string
	topicslices map[string][]int32
}

func (fi *Instance) String() string {
	return fmt.Sprintf("Instance{i: %v, fname: %v, topic slices: %v}", fi.i, fi.fname, fi.topicslices)
}

func NewInstance(i int, fname string) *Instance {
	return &Instance{i: i, fname: fname, topicslices: make(map[string][]int32)}
}

// PeerSched is a mapping from peernames to a slice of function instance
// definitions that should run on that peer.
type PeerSched map[string][]*Instance

func (ps PeerSched) Instances(name string) ([]*Instance, bool) {
	fi, found := ps[name]
	return fi, found
}

func init() {
	gob.Register(Ping{})
	gob.Register(Pong{})
	gob.Register(Vote{})
	gob.Register(Election{})
	gob.Register(PeerState{})
	gob.Register(Peer{})
	gob.Register(PeerSched{})
	gob.Register(Instance{})
	gob.Register(CmdMesg{})

	gob.Register(Follower) // register the Rank type def
	gob.Register(Timeout)  //register the Health type def
}
