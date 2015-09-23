ring
====

Ring represents a set of actor definitions used to divide a data
space into disjoint parts, each part owned by a particular actor
in the ring.

It then provides methods of routing messages to particular 
members of the ring. Its implementation does not maintain
state in Etcd or any other persistent store, it only defines
the names of the members.

### Quick Introduction

Each member of the name will have the same name prefix, for example:

    r := ring.New("consumers", 18, g)

Will create a set of 18 actor definitions, where the actors are
named `consumer-0`, `consumer-1`, etc.

### For Creation of Actors

The ring should be used anywhere when someone needs to create the
memebrs of the ring:

    func main() {
    	...

    	r := ring.New("consumers", 18, g)
    	for _, def := range r.ActorDefs() {
    		g.StartActor(def)
    	}

    	...
    }

### For Sending To Actors

And when someone needs to send to the members of the ring:

    func (p *producer) Act(g grid.Grid, exit <-chan bool) bool {
    	...

    	r := ring.New("consumers", 18, g)
    	p.tx.Send(r.ByHashedString("some-key"), "producer-message")
    }

### For Coordination of Actors

The [condition](../condition/) library supports ring with the `NameWatch`
condition. It can be used to watch when all the members of the ring join
a particular path:

    func (p *producer) Act(g grid.Grid, exit <-chan bool) bool {
    	j := condition.NewJoin(g.Etcd(), 10 * time.Minute, "finished", p.ID())
    	
    	... do work, until done, then ...

    	j.Join()
    }

    func (c *consumer) Act(g grid.Grid, exit <-chan bool) bool {
    	w := condition.NewNameWatch()
    	r := ring.New("producer", 20, g)

    	finished := w.WatchUntil(r)

    	for {
    		select {

    		... do work ...

    		case <- finished:
    			... all producers in ring have finished ...
    		}
    	}
    }