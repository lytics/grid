grid
====

Grid is a library to build distributed processes. It is a library not a container. It is simple to
use and provides the basic building blocks for distributed processing:

 1. Passing messages, which in grid is done via NATS.
 1. Coorinating the process instances, which in grid is done via ETCD.
 1. Scheduling tasks within the processes, which in grid is done via METAFORA.

### Some Examples

Staring the library is done in one line:

```go
func main() {
    ...
    g := grid.New(name, etcdservers, natsservers, taskmaker)
    g.Start()
    ... 
}
```

Scheduling work is done by calling `StartActor`:

```go
func main() {
    ...
    g := grid.New(name, etcdservers, natsservers, maker)
    g.Start()
    g.StartActor("counter")
    ...
}
```

Scheduled units of work are called actors, which are made by user code implementing the `ActorMaker` interface:

```go
type actormaker struct {}

func (m *actormaker) MakeActor(name string) (Actor, error) {
	if name == "counter" {
		return NewCounterActor(), nil
	} else {
		return NewOtherActor(), nil
	}
}
```

An actor is any Go type that implements the two methods of the `Actor` interface:

```go
type counteractor struct {
    id    string
    count int
}

func (a *counteractor) ID() {
    return a.id
}

func (a *counteractor) Act(g grid.Grid, exit <-chan bool) bool {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-exit:
            return true
        case <-ticker.C:
            log.Printf("Hello, I've counted %d times", a.count)
            count++
        }
    }
}
```

An actor can communicate with any other actor it knows the name of:

```go
func (a *counteractor) Act(g grid.Grid, exit <-chan bool) bool {
    c, err := grid.NewConn(a.id, g.Nats())
    if err != nil {
        log.Fatalf("%v: error: %v", a.id, err)
    }
    err = c.Send("other", "I have started counting")

    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-exit:
            return true
        case <-ticker.C:
            log.Printf("Hello, I've counted %d times", a.count)
            count++
        }
    }
}
```

```go
func (a *otheractor) Act(g grid.Grid, exit <-chan bool) bool {
    c, err := grid.NewConn(a.id, g.Nats())
    if err != nil {
        log.Fatalf("%v: error: %v", a.id, err)
    }
    for {
        select {
        case <-exit:
            return true
        case m := <-c.ReceiveC():
            switch m := m.(type) {
            case string:
                log.Printf("%v: got message: %v", a.id, m)
            }
        }
    }
}
```

An actor can schedule other actors:

```go
func (a *leaderactor) Act(g grid.Grid, exit <-chan bool) bool {
    for i:= 0; i<10; i++ {
        g.StartActor(fmt.Sprintf("follower-%d", i))
    }
}
```