package grid

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
)

// Actor is a stateful processing element in the grid.
type Actor interface {
	Act(in <-chan Event, state <-chan Event) <-chan Event
}

// NewActor creates a new actor giving it a name and an it.
type NewActor func(string, int) Actor

// Decoder decodes messages of topics the grid reads from.
type Decoder interface {
	New() interface{}
	Decode(d interface{}) error
}

// Encoder encodes messages of topics the grid writes to.
type Encoder interface {
	Encode(e interface{}) error
}

// Event is the interface implemented by all messages flowing
// through the grid.
type Event interface {
	Offset() int64
	Topic() string
	Part() int32
	Key() []byte
	Message() interface{}
}

type event struct {
	offset  int64
	topic   string
	part    int32
	key     []byte
	message interface{}
}

func (e *event) Offset() int64 {
	return e.offset
}

func (e *event) Topic() string {
	return e.topic
}

func (e *event) Part() int32 {
	return e.part
}

func (e *event) Key() []byte {
	return e.key
}

func (e *event) Message() interface{} {
	return e.message
}

// NewReadable is used to create an event suitable for reading.
func NewReadable(topic string, part int32, offset int64, message interface{}) Event {
	return &event{topic: topic, part: part, offset: offset, message: message}
}

// NewWritable is used to create an event suitable for writing.
func NewWritable(topic string, key []byte, message interface{}) Event {
	return &event{topic: topic, key: key, message: message}
}

// MinMaxOffset is used inform the min and max offsets for a given
// topic partition pair.
type MinMaxOffset struct {
	Topic string
	Part  int32
	Min   int64
	Max   int64
}

// UseOffset is used to indicate which offset in the interval [min,max]
// of available offsets to use for a particular topic partition pair.
type UseOffset struct {
	Topic  string
	Part   int32
	Offset int64
}

func NewUseOffset(topic string, part int32, offset int64) Event {
	return NewWritable("", nil, UseOffset{Topic: topic, Part: part, Offset: offset})
}

// Ready indicates that something is ready, and is used in multiple
// scenarios.
type Ready bool

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

func (p *Peer) Copy() *Peer {
	return &Peer{Name: p.Name, LastPongTs: p.LastPongTs}
}

func newPeer(name string, lastpong int64) *Peer {
	return &Peer{Name: name, LastPongTs: lastpong}
}

type PeerSet map[string]*Peer

func (ps PeerSet) Copy() PeerSet {
	newpeerset := make(map[string]*Peer)
	for name, peer := range ps {
		newpeerset[name] = peer.Copy()
	}
	return newpeerset
}

// PeerState is used to track the state of all the peers together
// as well as issue schedules.
type PeerState struct {
	// Indicates a new epoch for the cluster of peers.
	Epoch   uint64
	Term    uint32
	Version uint32
	Peers   PeerSet
	Sched   PeerSched
}

func (ps *PeerState) String() string {
	b, err := json.Marshal(ps)
	if err != nil {
		return ""
	}
	return string(b)
}

func (ps *PeerState) Copy() *PeerState {
	return &PeerState{Term: ps.Term, Version: ps.Version, Epoch: ps.Epoch, Peers: ps.Peers.Copy(), Sched: ps.Sched.Copy()}
}

func newPeerState() *PeerState {
	return &PeerState{Term: 0, Peers: make(map[string]*Peer)}
}

func newPeerStateCmdMsg(epoch uint64, gs *PeerState) *CmdMesg {
	return &CmdMesg{Epoch: epoch, Data: gs}
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

func (fi *Instance) Copy() *Instance {
	newtopicslices := make(map[string][]int32)
	for topic, parts := range fi.TopicSlices {
		newparts := make([]int32, len(parts))
		copy(newparts, parts)
		newtopicslices[topic] = newparts
	}
	return &Instance{Id: fi.Id, Fname: fi.Fname, TopicSlices: newtopicslices}
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

func (ps PeerSched) Copy() PeerSched {
	if ps == nil {
		return nil
	}

	newpeersched := PeerSched{}
	for name, insts := range ps {
		newinsts := make([]*Instance, len(insts))
		for i, inst := range insts {
			newinsts[i] = inst.Copy()
		}
		newpeersched[name] = newinsts
	}
	return newpeersched
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
