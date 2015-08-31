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

Scheduling work is done by the `Actor` abstraction:

    func main() {
        ...
        g := grid.New(name, etcdservers, natsservers, taskmaker)
        g.Start()
        g.StartActor("hello-world-actor")
        ...
    }

An actor is any Go type that implements the two methods of the `Actor` interface:

    type helloworld struct {
        id    string
        count int
    }

    func (h *helloworld) ID() {
        return h.id
    }

    func (h *helloworld) Act(g grid.Grid, exit <-chan bool) bool {
    	fmt.Println("hello world... I'm done now")
    	return true
    }

An actor can communicate with other actors:

    