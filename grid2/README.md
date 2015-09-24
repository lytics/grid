grid
====

Grid is a library to build distributed processes. A library in contrast to being a container. 
It is  simple to use and provides the basic building blocks for distributed processing:

 1. Passing messages, which in grid is done via NATS.
 1. Coordinating task instances, which in grid is done via ETCD.
 1. Scheduling tasks across the processes, which in grid is done via METAFORA.

### Quick Introduction

Configuring and starting are done in two lines:
```go
g := grid.New(name, etcdservers, natsservers, actormaker)
g.Start()
```

Scheduling work is done by calling StartActor:
```go
g.StartActor(grid.NewActorDef("hello"))
```

Scheduled units of work are called actors, which are made by user code implementing the ActorMaker interface:
```go
func (m *actormaker) MakeActor(def grid.ActorDef) (Actor, error) {
    swtich def.Type {
    case "hello":
        return &HelloActor{def}, nil
    case "other":
        return &OtherActor{def}, nil
    }
}
```

An actor is any Go type that implements the two methods of the Actor interface:
```go
type Actor interface {
    ID() {
    Act(g grid.Grid, exit <-chan bool) bool {
}
```

An actor can communicate with any other actor it knows the name of:
```go
func (a *helloactor) Act(g grid.Grid, exit <-chan bool) bool {
    tx, _ := grid.NewSender(g.Nats(), ...)
    tx.Send("other", "hello")
    return true
}

func (a *otheractor) Act(g grid.Grid, exit <-chan bool) bool {
    rx, _ := grid.NewReceiver(g.Nats(), ...)
    for {
        select {
        case <-exit:
            return true
        case m := <-rx.Msgs():
            log.Printf("got message: %v", m)
            return true
        }
    }
}
```

An actor can schedule other actors:
```go
func (a *leaderactor) Act(g grid.Grid, exit <-chan bool) bool {
    for i:= 0; i<10; i++ {
        name := fmt.Sprintf("follower-%d", i)
        g.StartActor(grid.NewActorDef(name).DefineType("follower"))
    }
    return true
}
```

Each actor has access to Etcd for state and coordination:
```go
func (a *statefulactor) Act(g grid.Grid, exit <-chan bool) bool {
    ttl := uint64(30)
    loc := strings.Join([]string{g.Name(), "state", a.ID()}, "/")
    etc := g.Etcd()
    etc.Create(loc, "state", ttl)
    return true
}
```

Each actor can use the Sender and Receiver interfaces for communication, but it
can also use Nats directly:
```go
func (a *otheractor) Act(g grid.Grid, exit <-chan bool) bool {
    g.Nats().Publish("announce", "hello")
    return true
}
```

### Ring and Condition

Two helper libraries are included to help with coordination tasks, [condition](condition/), and
message sending tasks for divide and conquer, [ring](ring/).

### Sender and Receiver

Though NATS can be used directly, two interface are available for convenience. 
`Sender` provides a buffered sender with ack requests and resends. `Receiver`
provides a receiver that acks, and can start multiple subscriber go-routines
internally to read incoming messages.

### Getting Started With Examples

Running the examples requires Etcd and Nats services running on `localhost`. Do the following to
get both running on your system:

Etcd, get and run:

    $ go get -u github.com/coreos/etcd
    $ etcd
    2015/08/31 11:06:08 etcdmain: listening for client requests on http://localhost:2379
    ...

Nats, get and run:

    $ go get -u github.com/nats-io/gnatsd
    $ gnatsd
    2015/08/31 11:07:19.925048 [INF] Listening for client connections on 0.0.0.0:4222
    ...

The [firstgrid](example/firstgrid/) example is the simplest. It starts two actors and they pass messages.
Very little error checking is done but the basics are all there.

The [flowgrid](example/flowgrid/) example is much more involved, but more realistic as it deals with
coordination using the [condition](condition/) library and [ring](ring/) library. It also adds random
failure to each actor to simulate how dealing with such failures can be dealt with using the 
[dfa](http://github.com/lytics/dfa) library.