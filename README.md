grid
====

Grid is a library to build distributed processes. It is a library not a container. It is simple to
use and provides the basic building blocks for distributed processing:

 1. Passing messages, which in grid is done via NATS.
 1. Coorinating the process instances, which in grid is done via ETCD.
 1. Scheduling tasks within the processes, which in grid is done via METAFORA.

### Basic Gists

Staring the library is done in one line:

```go
func main() {
    g := grid.New(name, etcdservers, natsservers, taskmaker)
    g.Start()
    ... 
}
```

Scheduling work is done by calling `StartActor`:

```go
func main() {
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
    c.Send("other", "I have started counting")

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
    ...
}
```

Each actor has access to Etcd for state and coordination:

```go
func (a *counteractor) Act(g grid.Grid, exit <-chan bool) bool {
	ttl := 30 // seconds
	_, err := g.Etcd().Create(fmt.Sprintf("/%v/state/%v", g.name(), a.id), "0", ttl)
	if err != nil {
		log.Fatalf("%v: error: %v", a.id, err)
	}
	...
}
```

Each actor can use the bidiractional `Conn` interface for communication, but it
can also use Nats directly:

```go
func (a *otheractor) Act(g grid.Grid, exit <-chan bool) bool {
	g.Nats().Publish("announce", "Staring...")
	...
}
```

## Getting Started With Examples

Running the examples requires Etcd and Nats services running on `localhost`. Do the following to
get both running on your system:

Etcd:

    $ go get github.com/coreos/etcd
    $ etcd
    2015/08/31 11:06:08 etcdmain: setting maximum number of CPUs to 1, total number of available CPUs is 8
    2015/08/31 11:06:08 etcdmain: no data-dir provided, using default data-dir ./default.etcd
    ...

Nats:

    $ go get github.com/nats-io/gnatsd
    $ gnatsd
    [19416] 2015/08/31 11:07:19.925002 [INF] Starting gnatsd version 0.6.1.beta
    [19416] 2015/08/31 11:07:19.925048 [INF] Listening for client connections on 0.0.0.0:4222
    [19416] 2015/08/31 11:07:19.925278 [INF] gnatsd is ready
    ...

With those services running, checkout the [example](example/)s directory. The [firstgrid](example/firstgrid/)
example is the simplest. It starts two actors and they pass messages. The [gridbench](example/gridbench)
example is more involved, it uses two helper libraries, [condition](condition/) and [ring](ring/) to
help coordinate actors, and disstribute messages to them.