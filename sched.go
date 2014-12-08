package grid

// peersched creates the schedule of which function instance should run on which peer.
func peersched(peers map[string]*Peer, ops map[string]*op, parts map[string][]int32) PeerSched {

	sched := PeerSched{}

	// Every peer should get some function instances, so just
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
				finsts[i].TopicSlices[topic] = make([]int32, 0)
			}
		}

		// For each topic or each instance, steal one partition from the
		// remaining partitions of that topic. This basically round-
		// robins the partitions of a topic to the instance of
		// functions.
		for topic, _ := range op.inputs {
			tparts := make([]int32, len(parts[topic]))
			copy(tparts, parts[topic])

			for i := 0; i < len(tparts); i++ {
				finsts[i%op.n].TopicSlices[topic] = append(finsts[i%op.n].TopicSlices[topic], tparts[i])
			}
		}

		// Round-robin each function instance to the peers. Basically
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
