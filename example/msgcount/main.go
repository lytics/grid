package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

var (
	etcdconnect = flag.String("etcd", "http://127.0.0.1:2379", "comma separated list of etcd urls to servers")
	natsconnect = flag.String("nats", "nats://127.0.0.1:4222", "comma separated list of nats urls to servers")
	readers     = flag.Int("readers", 3, "number of readers")
	counters    = flag.Int("counters", 2, "number of counters")
	messages    = flag.Int("messages", 5000000, "number of messages for each reader to send")
	nodes       = flag.Int("nodes", 1, "number of nodes in the grid")
)

func main() {
	runtime.GOMAXPROCS(4)

	flag.Parse()

	etcdservers := strings.Split(*etcdconnect, ",")
	natsservers := strings.Split(*natsconnect, ",")

	conf := &Conf{
		GridName:   "msgcount",
		NrMessages: *messages,
		NrReaders:  *readers,
		NrCounters: *counters,
	}

	m, err := newActorMaker(conf)
	if err != nil {
		log.Fatalf("error: failed to make actor maker: %v", err)
	}

	g := grid.New(conf.GridName, etcdservers, natsservers, m)

	exit, err := g.Start()
	if err != nil {
		log.Fatalf("error: failed to start grid: %v", err)
	}

	select {
	case <-condition.NodeJoin(g.Etcd(), exit, conf.GridName, "grid", *nodes):
		log.Printf("Starting actors")
	case <-exit:
		log.Printf("Shutting down, grid exited")
		return
	}

	for i := 0; i < conf.NrReaders; i++ {
		name := NewName("reader", i)
		err := g.StartActor(name)
		if err != nil {
			log.Fatalf("error: failed to start: %v", name)
		}
	}

	for i := 0; i < conf.NrCounters; i++ {
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

func NewName(role string, part int) string {
	return fmt.Sprintf("%v.%v", role, part)
}
