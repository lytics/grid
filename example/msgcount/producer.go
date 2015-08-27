package main

import (
	"math/rand"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
)

func NewProducerActor(id string, conf *Conf) grid.Actor {
	return &ProducerActor{id: id, conf: conf}
}

type ProducerActor struct {
	id   string
	conf *Conf
}

func (a *ProducerActor) ID() string {
	return a.id
}

func (a *ProducerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	r := ring.New("consumer", a.conf.NrConsumers, g)

	// Make some random length string data.
	dm := NewDataMaker(a.conf.MinSize, a.conf.MinCount)
	go dm.Start(exit)

	// Watch the leader. If leader exits, then exist also.
	j := condition.NewJoinWatch(g.Etcd(), exit, g.Name(), "leader")

	// Number of messages sent.
	n := 0
	for {
		select {
		case <-exit:
			return true
		case <-j.WatchExit():
			return true
		case <-j.WatchError():
			return true
		case d := <-dm.Data():
			n++
			c.Send(r.ByInt(n), &DataMsg{From: a.id, Data: d})
		case m := <-c.ReceiveC():
			switch m.(type) {
			case SendResultMsg:
				// There is a cycle in communication
				// between leader and us, don't
				// block on the send.
				go c.Send("leader", &ResultMsg{Producer: a.id, Count: n})
			}
		}
	}
}

func NewDataMaker(minsize, mincount int) *datamaker {
	s := int64(0)
	for i := 0; i < 10000; i++ {
		s += time.Now().UnixNano()
	}
	dice := rand.New(rand.NewSource(s))
	size := minsize + dice.Intn(minsize)
	count := mincount + dice.Intn(mincount)
	return &datamaker{
		size:    size,
		count:   count,
		dice:    dice,
		output:  make(chan string),
		letters: []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
	}
}

type datamaker struct {
	size    int
	count   int
	dice    *rand.Rand
	output  chan string
	letters []rune
}

func (d *datamaker) Data() <-chan string {
	return d.output
}

func (d *datamaker) Start(exit <-chan bool) {
	makedata := func(n int) string {
		b := make([]rune, n)
		for i := range b {
			b[i] = d.letters[d.dice.Intn(len(d.letters))]
		}
		return string(b)
	}
	sent := 0
	for {
		select {
		case <-exit:
			return
		default:
			if sent >= d.count {
				return
			}
			select {
			case <-exit:
				return
			case d.output <- makedata(d.dice.Intn(d.size)):
			}
		}
	}
}
