package grid

// peersched creates the schedule of which actor instance should run on which peer.
func peersched(peers map[string]*Peer, actorconfs map[string]*actorconf, parts map[string][]int32, statetopic string) PeerSched {

	sched := PeerSched{}

	// Every peer should get some actor instances, so just
	// initialize the map of peers and their slice of
	// actor instances upfront.
	for peer, _ := range peers {
		sched[peer] = make([]*Instance, 0)
	}

	// We go actor-configuration by actor-configuration. Note that this is
	// different than going actor by actor, because each configuration
	// will configure N actor instances.
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
			if topic == statetopic {
				// For the state-topic, every actor instance gets all the partitions, in
				// otherwords, they all read the full topic. Each can hence recreate the
				// state of the other. This is important in the cases where the user
				// changes the number of instances of an actor, and new instances need
				// to take care of work that in a previous version of the grid was
				// being done by no longer existing actors.
				for i := 0; i < actorconf.n; i++ {
					tparts := make([]int32, len(parts[topic]))
					copy(tparts, parts[topic])
					actors[i].TopicSlices[topic] = tparts
				}
			} else {
				// For a normal, non-state topic, the partitions are just round-robined
				// to the actor instances until the topic is being read in full.
				tparts := make([]int32, len(parts[topic]))
				copy(tparts, parts[topic])
				for i := 0; i < len(tparts); i++ {
					actors[i%actorconf.n].TopicSlices[topic] = append(actors[i%actorconf.n].TopicSlices[topic], tparts[i])
				}
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

		// Now move on to the next actor configuration...
	}

	return sched
}
