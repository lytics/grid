package grid

import "fmt"

// TopicSlice is the partitions from a topic that a particular
// running instance of 'f' will read from.
type TopicSlice struct {
	topic string
	parts []uint32
}

func (ts *TopicSlice) String() string {
	return fmt.Sprintf("TopicSlice{topic: %v, parts: %v}", ts.topic, ts.parts)
}

func NewTopicSlice(topic string) *TopicSlice {
	return &TopicSlice{topic: topic, parts: make([]uint32, 0)}
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

// HostSched is a mapping from hostnames to a slice of function instance
// definitions that should run on that host.
type HostSched map[string][]*FuncInst

func (hs HostSched) FunctionInstances(host string) ([]*FuncInst, bool) {
	fi, found := hs[host]
	return fi, found
}

// hostsched creates the schedule of which function instance should run on which host.
func hostsched(hosts map[string]bool, ops map[string]*op, partitions map[string][]uint32) HostSched {

	sched := HostSched{}

	// Every host shold get some function instances, so just
	// initialize the map of hosts and their slice of
	// function instances upfront.
	for host, _ := range hosts {
		sched[host] = make([]*FuncInst, 0)
	}

	for fname, op := range ops {

		// Every function will have N instances of it running
		// somewhere on the grid. Also, each instance can
		// read from multiple topics, but only from a sub
		// set of the partitions.

		finsts := make([]*FuncInst, op.n)
		for i := 0; i < op.n; i++ {
			finsts[i] = NewFuncInst(i, fname)
			for _, topic := range op.inputs {
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
		for _, topic := range op.inputs {
			parts := make([]uint32, len(partitions[topic]))
			copy(parts, partitions[topic])

			for i := 0; i < len(parts); i++ {
				finsts[i%op.n].topicslices[topic].parts = append(finsts[i%op.n].topicslices[topic].parts, parts[i])
			}
		}

		// Round-robin each function instance to the hosts. Basially
		// each host steals one function instance until non-remain.
		i := len(finsts) - 1
		for i >= 0 {
			for host, _ := range hosts {
				if i < 0 {
					continue
				}
				sched[host] = append(sched[host], finsts[i])
				i--
			}
		}

		// Now move on to the next function...
	}

	return sched
}
