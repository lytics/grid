condition
=========

Common usage patterns for Etcd to implement group join, exit, and watch counts.
Things that can be done with the condition library:

 1. Register that someone has joined a process.
 1. Watch the join, and signal when they leave.
 1. Watch a group of joins, until some count condition is satisfied.

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