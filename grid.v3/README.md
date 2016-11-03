grid
====

Grid is a library for doing distributed processing. It's main goal is to help
in scheduling fine-grain stateful computations, which grid calls actors, and
sending data between them.

## Grid
Anything that implements the `Grid` interface is a grid. In grid an actor
is typically just a go-routin or set of go-routines performing some stateful
computation.

```go
type Grid interface {
    MakeActor(def *ActorDef) (Actor, error)
}
```

## Example Grid
Below is a basic example of starting your grid application. Error checking and
full parameters are omitted with "..." placeholders.

```go
type MyApp struct {
    MakeActor(def *grid.ActorDef) (grid.Actor, error) {
        ...
    }
}

func main() {
    etcd, err := etcdv3.New(...)
    ...

    g, err := grid.NewServer(etcd, "myapp", MyApp{})
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
is started when the `Serve` method is called on the server. No matter how many
processes are participating in the grid, only one leader actor is started, it
is a singleton.

```go
const timeout = 2 * time.Second

type leader struct {
    ...
}

func (a *leader) Act(c context.Context) {
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
        def := grid.NewActorDef("worker"+i)
        def.Type = "worker"

        // Start a new actor on the given peer.
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

type worker struct {
    ...
}

func (a *worker) Act(c context.Context) {
    id, err := grid.ContextActorID(c)
    ...

    mailbox, err := grid.NewMailbox(c, id, size)
    ...
    defer mailbox.Close()

    for {
        select {
        case <-c.Done():
            return
        case envelope := <-mailbox.C:
            switch msg := envelope.Msg.(type) {
            ... do some work ...
            }
        }
    }
}
```

## Kubernetes + Grid
The examples above are meant to give some intuative sense of what the grid
library does. Howevery what it does not do is:

 1. Package up your congifuration and binaries
 1. Start your VMs
 1. Start your processes on those VMs
 1. Autoscale your VMs when resource run low
 1. Reschedule crashed processes
 1. etc...

This is intentional as other tools already do these things. At the top of
our list is Kubernetes and Docker, which between the two perform all of the
above.

Grid comes into the picture once you start building out your application logic
and need things like coordination and messaging, which under the hood in grid
is done with Etcd and gRPC - taking care of some boilerplate code for you.

## Client
There are times you will need to talk to grid actors from non-actors, the
`Client` can be used in such a case.

```go

func SomeExternalCode() {
    etcd, err := etcdv3.New(...)
    ...

    client, err := grid.NewClient("myapp", etcd)
    ...

    const timeout = 2 * time.Second

    res, err := client.Request(timeout, "some-mailbox-name", &MyMsg{
        ...
    })

    ... process the response ...
}
```