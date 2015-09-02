condition
=========

Common usage patterns for Etcd to implement group join, exit, and watch counts.
Things that can be done with the condition library:

 1. Register that someone has joined a process.
 1. Watch the join, and signal when they leave.
 1. Watch a group of joins, until some count condition is satisfied.
 1. Store and fetch state with CAS operations and stale read flag.

### Join Example
 ```go
    j := condition.NewJoin(g.Etcd(), 30*time.Second, "registration", "path", "in", "etcd")
    j.Join()

    ... do work ...

    j.Exit()
}
```

### WatchJoin Example
```go
	w := condition.NewJoinWatch(g.Etcd(), exit, "registration", "path", "to", "watch")
	<-w.WatchJoin()
	log.Printf("other participant has joined")

	<-w.WatchExit()
	log.Printf("other participant has exited")
```

### CountWatch Example
```go
	w := condition.NewCountWatch(g.Etcd(), exit, "registration", "path", "to", "watch")
	<-w.WatchUntil(10)
	log.Printf("all ten participants have joined")

	<-w.WatchUntil(0)
	log.Printf("all ten participants have exited")

	for {
		select {
			case n := <-w.WatchCount():
				log.Printf("there are %d participants registered", n)
		}
	}
```

### State Example
```go
	s := condition.NewState(g.Etcd(), 30*time.Second, "path", "to", "saved", "state")
	err := s.Init(state)
	if err != nil {
		// State failed to be created.
	}

	stale, err := s.Store(state)
	if stale {
		// State failed to save because of a stale view of the state.
	}
	if err != nil {
		// State failed to save becuase of another type of error.
	}

	stale, err := s.Fetch(state)
	if stale {
		// State was read, but its Etcd index indicates that updates happened.
	}
	if err != nil {
		// State failed to read because of deserialization error.
	}

	stale, err := s.Remove()
	if stale {
		// State failed to remove because of a stale view of the state.
	}
	if err != nil {
		// State failed to remove for another type of error.
	}
```