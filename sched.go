package grid

import "fmt"

// TopicSlice is the partitions from a topic that a particular
// running instance of 'f' will read from.
type TopicSlice struct {
	topic string
	parts []int32
}

func (ts *TopicSlice) String() string {
	return fmt.Sprintf("TopicSlice{topic: %v, parts: %v}", ts.topic, ts.parts)
}

func NewTopicSlice(topic string) *TopicSlice {
	return &TopicSlice{topic: topic, parts: make([]int32, 0)}
}

// FuncInst is the full mapping of topic slices that a particular
// running instance of 'f' will read from, since a 'f' could read
// from multiple topics.
type FuncInst struct {
	i           int
	fname       string
	topicslices map[string]*TopicSlice
}

func (fi *FuncInst) String() string {
	return fmt.Sprintf("FuncInst{i: %v, fname: %v, topic slices: %v}", fi.i, fi.fname, fi.topicslices)
}

func NewFuncInst(i int, fname string) *FuncInst {
	return &FuncInst{i: i, fname: fname, topicslices: make(map[string]*TopicSlice)}
}

// PeerSched is a mapping from peernames to a slice of function instance
// definitions that should run on that peer.
type PeerSched map[string][]*FuncInst

func (hs PeerSched) FunctionInstances(name string) ([]*FuncInst, bool) {
	fi, found := hs[name]
	return fi, found
}

func (hs PeerSched) FunctionInstancesNames(name string) map[string]bool {
	fnames := make(map[string]bool)
	for _, fi := range hs[name] {
		fnames[fi.fname] = true
	}
	return fnames
}

// peersched creates the schedule of which function instance should run on which peer.
func peersched(peers map[string]*Peer, ops map[string]*op, parts map[string][]int32) PeerSched {

	sched := PeerSched{}

	// Every peer shold get some function instances, so just
	// initialize the map of peers and their slice of
	// function instances upfront.
	for peer, _ := range peers {
		sched[peer] = make([]*FuncInst, 0)
	}

	for fname, op := range ops {

		// Every function will have N instances of it running
		// somewhere on the grid. Also, each instance can
		// read from multiple topics, but only from a sub
		// set of the partitions.

		finsts := make([]*FuncInst, op.n)
		for i := 0; i < op.n; i++ {
			finsts[i] = NewFuncInst(i, fname)
			for topic, _ := range op.inputs {
				// For every instance create its "topic slice" for
				// each topic it reads from. A "topic slice" is
				// just a topic name and a slice partition numbers.
				finsts[i].topicslices[topic] = NewTopicSlice(topic)
			}
		}

		// For each topic or each instance, steal one partition from the
		// ramaining partitions of that topic. This basically round-
		// robins the partitions of a topic to the instance of
		// functions.
		for topic, _ := range op.inputs {
			ps := make([]int32, len(parts[topic]))
			copy(ps, parts[topic])

			for i := 0; i < len(ps); i++ {
				finsts[i%op.n].topicslices[topic].parts = append(finsts[i%op.n].topicslices[topic].parts, ps[i])
			}
		}

		// Round-robin each function instance to the peers. Basially
		// each peer steals one function instance until none remain.
		i := len(finsts) - 1
		for i >= 0 {
			for peer, _ := range peers {
				if i < 0 {
					continue
				}
				sched[peer] = append(sched[peer], finsts[i])
				i--
			}
		}

		// Now move on to the next function...
	}

	return sched
}
