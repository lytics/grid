package grid

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
)

// Event is the interface implemented by all messages flowing
// through the grid.
type Event interface {
	Offset() int64
	Topic() string
	Key() string
	Message() interface{}
}

type event struct {
	offset  int64
	topic   string
	key     string
	message interface{}
}

func (e *event) Offset() int64 {
	return e.offset
}

func (e *event) Topic() string {
	return e.topic
}

func (e *event) Key() string {
	return e.key
}

func (e *event) Message() interface{} {
	return e.message
}

// NewReadable is used to create an event suitable for reading.
func NewReadable(topic string, offset int64, message interface{}) Event {
	return &event{topic: topic, offset: offset, message: message}
}

// NewWritable is used to create an event suitable for writing.
func NewWritable(topic, key string, message interface{}) Event {
	return &event{topic: topic, key: key, message: message}
}

// CmdMesg is an envelope for more specific messages on the command topic.
type CmdMesg struct {
	Epoch uint64
	Data  interface{}
}

func newCmdMesg(epoch uint64, data interface{}) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: data}
}

func (m *CmdMesg) String() string {
	return fmt.Sprintf("CmdMesg{Epoch: %v, Data: %v}", m.Epoch, m.Data)
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

// Rank is the rank of any given peer.
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

func newPing(epoch uint64, leader string, term uint32) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: Ping{Leader: leader, Term: term}}
}

func (p Ping) String() string {
	return fmt.Sprintf("Ping{Leader: %v, Term: %d}", p.Leader, p.Term)
}

// Pong is send only by followers in response to a leader's Ping.
type Pong struct {
	Follower string
	Term     uint32
}

func newPong(epoch uint64, follower string, term uint32) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: Pong{Follower: follower, Term: term}}
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

func newVote(epoch uint64, candidate string, term uint32, from string) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: Vote{Term: term, Candidate: candidate, From: from}}
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

func newElection(epoch uint64, candidate string, term uint32) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: Election{Term: term, Votes: 1, Candidate: candidate}}
}

func (e Election) Copy() *Election {
	return &Election{Term: e.Term, Votes: e.Votes, Candidate: e.Candidate}
}

func (e Election) String() string {
	return fmt.Sprintf("Election{Term: %d, Votes: %d, Candidate: %v}", e.Term, e.Votes, e.Candidate)
}

// Peer is use to track liveness of each peer.
type Peer struct {
	Name       string
	LastPongTs int64
}

func newPeer(name string, lastpong int64) *Peer {
	return &Peer{Name: name, LastPongTs: lastpong}
}

// PeerState is used to track the state of all the peers together
// as well as issue schedules.
type PeerState struct {
	Term    uint32
	Version uint32
	Sched   PeerSched
	Epoch   uint64 // indicates a new epoch for the cluster of peers
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

func newPeerStateMsg(epoch uint64, peerstate *PeerState) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: peerstate}
}

// Instance is the full mapping of topic slices that a particular
// running instance of 'f' will read from, since a 'f' could read
// from multiple topics.
type Instance struct {
	Id          int
	Fname       string
	TopicSlices map[string][]int32
}

func (fi *Instance) String() string {
	return fmt.Sprintf("Instance{i: %v, fname: %v, topic slices: %v}", fi.Id, fi.Fname, fi.TopicSlices)
}

func NewInstance(i int, fname string) *Instance {
	return &Instance{Id: i, Fname: fname, TopicSlices: make(map[string][]int32)}
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
	gob.Register(Follower)
}
