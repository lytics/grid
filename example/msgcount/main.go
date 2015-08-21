package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lytics/grid"
)

const (
	GridName = "msgcount"
)

func init() {
	gob.Register(CntMsg{})
	gob.Register(DoneMsg{})
}

var (
	etcdconnect = flag.String("etcd", "http://127.0.0.1:2379", "comma separated list of etcd urls to servers")
	natsconnect = flag.String("nats", "nats://127.0.0.1:4222", "comma separated list of nats urls to servers")
	readers     = flag.Int("readers", 3, "number of readers")
	counters    = flag.Int("counters", 2, "number of counters")
	messages    = flag.Int("messages", 5000000, "number of messages for each reader to send")
)

func main() {
	runtime.GOMAXPROCS(4)

	flag.Parse()

	etcdservers := strings.Split(*etcdconnect, ",")
	natsservers := strings.Split(*natsconnect, ",")

	m, err := newActorMaker(*messages, *counters, *readers)
	if err != nil {
		log.Fatalf("error: failed to make actor maker: %v", err)
	}

	g := grid.New(GridName, etcdservers, natsservers, m)

	exit, err := g.Start()
	if err != nil {
		log.Fatalf("error: failed to start grid: %v", err)
	}

	for i := 0; i < *readers; i++ {
		name := NewName("reader", i)
		err := g.StartActor(name)
		if err != nil {
			log.Fatalf("error: failed to start: %v", name)
		}
	}

	for i := 0; i < *counters; i++ {
		name := NewName("counter", i)
		err := g.StartActor(name)
		if err != nil {
			log.Fatalf("error: failed to start: %v", name)
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sig:
		log.Printf("Shutting down")
		g.Stop()
	case <-exit:
		log.Printf("Shutting down, grid exited")
	}
}

type DoneMsg struct {
	From string
}

type CntMsg struct {
	From   string
	Number int
}

func NewName(role string, part int) string {
	return fmt.Sprintf("%v.%v", role, part)
}

type maker struct {
	nmessages int
	ncounters int
	nreaders  int
}

func newActorMaker(nmessages, ncounters, nreaders int) (*maker, error) {
	if nreaders > 100 {
		return nil, fmt.Errorf("to many reader actors requested: %v", nreaders)
	}
	if ncounters > 100 {
		return nil, fmt.Errorf("to many counter actors requested: %v", ncounters)
	}
	return &maker{
		nmessages: nmessages,
		ncounters: ncounters,
		nreaders:  nreaders,
	}, nil
}

func (m *maker) MakeActor(id string) (grid.Actor, error) {
	rprefix := fmt.Sprintf("%v.%v", GridName, "reader")
	cprefix := fmt.Sprintf("%v.%v", GridName, "counter")

	switch {
	case strings.Index(id, rprefix) >= 0:
		log.Printf("making new reader actor: %v", id)
		return NewReaderActor(id, m.nmessages, m.ncounters), nil
	case strings.Index(id, cprefix) >= 0:
		log.Printf("making new counter actor: %v", id)
		return NewCounterActor(id, m.nmessages, m.ncounters), nil
	default:
		return nil, fmt.Errorf("name does not map to any type of actor: %v", id)
	}
}

func NewReaderActor(id string, nmessages, ncounters int) grid.Actor {
	return &ReaderActor{id: id, nmessages: nmessages, ncounters: ncounters}
}

type ReaderActor struct {
	id        string
	ncounters int
	nmessages int
}

func (a *ReaderActor) ID() string {
	return a.id
}

func (a *ReaderActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	n := 0
	start := time.Now()
	for {
		select {
		case <-exit:
			return true
		default:
			if n < a.nmessages {
				err := c.Send(ByModuloInt("counter", n, a.ncounters), &CntMsg{From: a.id, Number: n})
				if err != nil {
					log.Fatalf("%v: error: %v", a.id, err)
				}
				n++
				if n%100000 == 0 {
					log.Printf("%v: sent: %v, rate: %.2f/sec", a.id, n, float64(n)/time.Now().Sub(start).Seconds())
				}
			} else {
				for i := 0; i < a.ncounters; i++ {
					err := c.Send(ByNumber("counter", i), &DoneMsg{From: a.id})
					if err != nil {
						log.Fatalf("%v: error: %v", a.id, err)
					}
				}
				log.Printf("%v: finished", a.id)
				return true
			}
		}
	}
}

func NewCounterActor(id string, nmessages, ncounters int) grid.Actor {
	parts := strings.Split(id, ".")
	nr, err := strconv.Atoi(parts[2])
	if err != nil {
		log.Printf("%v: fatal: %v", id, err)
		return nil
	}
	return &CounterActor{id: id, nr: nr, nmessages: nmessages, ncounters: ncounters}
}

type CounterActor struct {
	id        string
	nr        int
	nmessages int
	ncounters int
}

func (a *CounterActor) ID() string {
	return a.id
}

func (a *CounterActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	counts := make(map[string]map[int]bool)
	for {
		select {
		case <-exit:
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case CntMsg:
				bucket, ok := counts[m.From]
				if !ok {
					bucket = make(map[int]bool)
					counts[m.From] = bucket
				}
				bucket[m.Number] = true
			case DoneMsg:
				bucket := counts[m.From]
				missing := 0
				for i := 0; i < a.nmessages; i++ {
					if i%a.ncounters == a.nr {
						if !bucket[i] {
							missing++
						}
					}
				}
				log.Printf("%v: from actor: %v, missing: %v", a.id, m.From, missing)
			}
		}
	}
}

func ByModuloInt(role string, key int, n int) string {
	part := key % n
	return fmt.Sprintf("%s.%s.%d", GridName, role, part)
}

func ByNumber(role string, part int) string {
	return fmt.Sprintf("%s.%s.%d", GridName, role, part)
}
