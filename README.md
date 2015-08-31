grid
====

Grid is a library to build distributed processes. It is a library not a container. It is simple to
use and provides the basic building blocks for distributed processing:

 1. Passing messages, which in grid is done via NATS.
 1. Coorinating the process instances, which in grid is done via ETCD.
 1. Scheduling tasks within the processes, which in grid is done via METAFORA.

### Some Examples

Staring the library is done in one line:

    func main() {
        ...
        g := grid.New(name, etcdservers, natsservers, taskmaker)
        g.Start()
        ... 
    }

Scheduling work is done by calling `StartActor`:

    func main() {
        ...
        g := grid.New(name, etcdservers, natsservers, taskmaker)
        g.Start()
        g.StartActor("hello-world-actor")
        ...
    }

An actor is any Go type that implements the two methods of the `Actor` interface:

    type countingactor struct {
        id    string
        count int
    }

    func (a *countingactor) ID() {
        return a.id
    }

    func (a *countingactor) Act(g grid.Grid, exit <-chan bool) bool {
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

An actor can communicate with any other actor it knows the name of:

    func (a *countingactor) Act(g grid.Grid, exit <-chan bool) bool {
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
    