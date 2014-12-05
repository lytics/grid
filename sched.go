package grid

import "fmt"

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

// PeerSched is a mapping from names to a slice of function instance
// definitions that should run on that peer.
type PeerSched map[string][]*Instance

func (ps PeerSched) Instances(name string) ([]*Instance, bool) {
	fi, found := ps[name]
	return fi, found
}

// peersched creates the schedule of which function instance should run on which peer.
func peersched(peers map[string]*Peer, ops map[string]*op, parts map[string][]int32) PeerSched {

	sched := PeerSched{}

	// Every peer shold get some function instances, so just
	// initialize the map of peers and their slice of
	// function instances upfront.
	for peer, _ := range peers {
		sched[peer] = make([]*Instance, 0)
	}

	for fname, op := range ops {

		// Every function will have N instances of it running
		// somewhere on the grid. Also, each instance can
		// read from multiple topics, but only from a sub
		// set of the partitions.

		finsts := make([]*Instance, op.n)
		for i := 0; i < op.n; i++ {
			finsts[i] = NewInstance(i, fname)
			for topic, _ := range op.inputs {
				// For every instance create its "topic slice" for
				// each topic it reads from. A "topic slice" is
				// just a topic name and a slice partition numbers.
				finsts[i].topicslices[topic] = make([]int32, 0)
			}
		}

		// For each topic or each instance, steal one partition from the
		// ramaining partitions of that topic. This basically round-
		// robins the partitions of a topic to the instance of
		// functions.
		for topic, _ := range op.inputs {
			tparts := make([]int32, len(parts[topic]))
			copy(tparts, parts[topic])

			for i := 0; i < len(tparts); i++ {
				finsts[i%op.n].topicslices[topic] = append(finsts[i%op.n].topicslices[topic], tparts[i])
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
