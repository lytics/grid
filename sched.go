package grid

// peersched creates the schedule of which actor instance should run on which peer.
func peersched(peers map[string]*Peer, actorconfs map[string]*actorconf, parts map[string][]int32) PeerSched {

	sched := PeerSched{}

	// Every peer should get some actor instances, so just
	// initialize the map of peers and their slice of
	// actor instances upfront.
	for peer, _ := range peers {
		sched[peer] = make([]*Instance, 0)
	}

	for name, actorconf := range actorconfs {

		// Every actor will have N instances of it running
		// somewhere on the grid. Also, each instance can
		// read from multiple topics, but only from a sub
		// set of the partitions.

		actors := make([]*Instance, actorconf.n)
		for i := 0; i < actorconf.n; i++ {
			actors[i] = NewInstance(i, name)
			for topic, _ := range actorconf.inputs {
				// For every instance create its "topic slice" for
				// each topic it reads from. A "topic slice" is
				// just a topic name and a slice of partition numbers.
				actors[i].TopicSlices[topic] = make([]int32, 0)
			}
		}

		// For each topic of each instance, steal one partition from the
		// remaining partitions of that topic. This basically round-
		// robins the partitions of a topic to the instance of
		// actors.
		for topic, _ := range actorconf.inputs {
			tparts := make([]int32, len(parts[topic]))
			copy(tparts, parts[topic])

			for i := 0; i < len(tparts); i++ {
				actors[i%actorconf.n].TopicSlices[topic] = append(actors[i%actorconf.n].TopicSlices[topic], tparts[i])
			}
		}

		// Round-robin each actor instance to the peers. Basically
		// each peer steals one actor instance until none remain.
		i := len(actors) - 1
		for i >= 0 {
			for peer, _ := range peers {
				if i < 0 {
					continue
				}
				sched[peer] = append(sched[peer], actors[i])
				i--
			}
		}

		// Now move on to the next actor...
	}

	return sched
}
