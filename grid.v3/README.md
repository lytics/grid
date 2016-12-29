grid
====

Grid is a library for doing distributed processing. It's main goal is to help
in scheduling fine-grain stateful computations, which grid calls actors, and
sending data between them. Its only external dependency is an Etcd v3 cluster.

## Grid
Anything that implements the `Grid` interface is a grid. The interface defines
a method to make actors from actor definitions. Actors are explained further
down in the readme.

```go
type Grid interface {
    MakeActor(def *ActorDef) (Actor, error)
}
```

## Example Grid
Below is a basic example of starting your grid application. The `MakeActor` method
must always know how to make a "leader". The leader actor will be started for you
when `Serve` is called. The leader is the entry-point of you application.

```go
type MyGrid struct {
    MakeActor(def *grid.ActorDef) (grid.Actor, error) {
        switch def.Type {
        case "leader":
            return &LeaderActor{}, nil
        }
    }
}

func main() {
    etcd, err := etcdv3.New(...)
    ...

    g, err := grid.NewServer(etcd, "mygrid", MyGrid{})
    ...

    lis, err := net.Listen("tcp", ...)
    ...

    g.Serve(lis)
}

```

## Actor
Anything that implements the `Actor` interface is an actor. Actors typically
represent the central work of you application.

```go
type Actor interface {
    Act(c context.Context)
}
```

## Example Actor, Part 1
Below is an actor that starts other actors, this is a typical way of structuring
an application with grid. In particular grid reqires that the implementor of
the `Grid` interface knows how to make an actor of name and type "leader", it
is started when the `Serve` method is called. No matter how many processes are
participating in the grid, only one leader actor is started, it is a singleton.
The "leader" actor can be thought of as an entry-point into you distributed
application.

```go
const timeout = 2 * time.Second

type LeaderActor struct {
    ...
}

func (a *LeaderActor) Act(c context.Context) {
    client, err := grid.ContextClient(c)
    ...

    peers, err := client.Peers(...)
    ...

    i := 0
    for _, peer := range peers {
        // Actor names are unique, registered in etcd.
        // There can never be more than one actor with
        // a given name. When an actor exits or panics
        // its record is removed from etcd.
        def := grid.NewActorDef("worker-%d", i)
        def.Type = "worker"

        // Start a new actor on the given peer. The
        // type "ActorDef" is special. When sent to
        // the mailbox of a peer, that peer will
        // start an actor based on the definition.
        res, err := client.Request(timeout, peer, def)
        ...
        i++
    }

    ...
}
```

## Example Actor, Part 2
An actor will typically need to receive data to work on. This may come
from the filesystem or a database, but it can also come from messages
sent to a mailbox. Just like actors a mailbox is unique by name. Etcd
is used to register the name and guarantee that only one such mailbox
exists.

```go
const size = 10

type WorkerActor struct {
    ...
}

func (a *WorkerActor) Act(c context.Context) {
    id, err := grid.ContextActorID(c)
    ...

    mailbox, err := grid.NewMailbox(c, id, size)
    ...
    defer mailbox.Close()

    for {
        select {
        case req := <-mailbox.C:
            switch req.Msg().(type) {
            case PingMsg:
                err := req.Respond(&PongMsg{
                    ...
                })
        }
    }
}
```

## Example Actor, Part 3
Each actor receives a context as a parameter in its `Act` method. That context
is created by the peer that started the actor. The context contains several
useful values, they can be extracted using the `Context*` functions.

```go
func (a *WorkerActor) Act(c context.Context) {
    // The ID of the actor.
    id, err := grid.ContextActorID(c)

    // The name of the actor, as given in ActorDef.
    name, err := grid.ContextActorName(c)

    // The namespace of the grid this actor is associated with.
    namespace, err := grid.ContextActorNamespace(c)

    // The etcd client associated with this actor.
    etcd, err := grid.ContextEtcd(c)

    // The grid client associated with this actor.
    client, err := grid.ContextClient(c)
}
```

## Example Actor, Part 4
An actor can exit whenever it wants, but it *must* exit when its context
signals done. An actor should always monitor its context Done channel.

```go
func (a *WorkerActor) Act(c context.Context) {
    for {
        select {
        case <-c.Done():
            // Stop requested, clean up and exit.
            return
        case ...
        }
    }
}
```

## Example Actor, Part 5
Each actor is registered into etcd. Consequently each actor's name acts like
a mutex. If code requests the actor to start *twice* the second request will
receive an error indicating that the actor is already started.

```go
const timeout = 2 * time.Second

func (a *LeaderActor) Act(c context.Context) {
    client, err := grid.ContextClient(c)

    def := grid.NewActorDef("worker-0")

    // First request to start.
    err = client.Request(timeout, peer, def)

    // Second request will fail, if the first succeeded.
    err = client.Request(timeout, peer, def)
}
```

## Kubernetes + Grid
The examples above are meant to give some intuitive sense of what the grid
library does. Howevery what it does not do is:

 1. Package up your congifuration and binaries
 1. Start your VMs
 1. Start your processes on those VMs
 1. Autoscale your VMs when resources run low
 1. Reschedule crashed processes
 1. etc...

This is intentional as other tools already do these things. At the top of
our list is Kubernetes and Docker, which between the two perform all of the
above.

Grid comes into the picture once you start building out your application logic
and need things like coordination and messaging, which under the hood in grid
is done with Etcd and gRPC - taking care of some boilerplate code for you.

## Client
There are situations where you will need to talk to grid actors from non-actors,
the `Client` can be used for this.

```go
const timeout = 2 * time.Second


func Example() {
    etcd, err := etcdv3.New(...)
    ...

    client, err := grid.NewClient(etcd, "myapp")
    ...

    res, err := client.Request(timeout, "some-mailbox-name", &MyMsg{
        ...
    })

    ... process the response ...
}
```