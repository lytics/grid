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
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
)

var (
	etcdconnect = flag.String("etcd", "http://127.0.0.1:2379", "comma separated list of etcd urls to servers")
	natsconnect = flag.String("nats", "nats://127.0.0.1:4222", "comma separated list of nats urls to servers")
	producers   = flag.Int("producers", 3, "number of producers")
	consumers   = flag.Int("consumers", 2, "number of consumers")
	nodes       = flag.Int("nodes", 1, "number of nodes in the grid")
	minsize     = flag.Int("minsize", 1000, "minimum message size, actual size will be in the range [min,2*min]")
	mincount    = flag.Int("mincount", 100000, "minimum message count, actual count will be in the range [min,2*min]")
)

func main() {
	runtime.GOMAXPROCS(4)

	flag.Parse()

	etcdservers := strings.Split(*etcdconnect, ",")
	natsservers := strings.Split(*natsconnect, ",")

	conf := &Conf{
		GridName:    "gridbench",
		MinSize:     *minsize,
		MinCount:    *mincount,
		NrProducers: *producers,
		NrConsumers: *consumers,
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("error: failed to discover hostname: %v", err)
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

	j := condition.NewJoin(g.Etcd(), 30*time.Second, g.Name(), "hosts", hostname)
	err = j.Join()
	if err != nil {
		log.Fatalf("error: failed to regester: %v", err)
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-exit:
				j.Exit()
				return
			case <-ticker.C:
				err := j.Alive()
				if err != nil {
					log.Fatalf("error: failed to report liveness: %v", err)
				}
			}
		}
	}()

	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "hosts")
	select {
	case <-exit:
		log.Printf("Shutting down, grid exited")
		return
	case <-w.WatchError():
		log.Fatalf("error: failed to watch other hosts join: %v", err)
	case <-w.WatchUntil(*nodes):
	}

	rp := ring.New("producer", conf.NrProducers, g)
	for _, name := range rp.Names() {
		err := g.StartActor(name)
		if err != nil {
			log.Fatalf("error: failed to start: %v", name)
		}
	}

	rc := ring.New("consumer", conf.NrConsumers, g)
	for _, name := range rc.Names() {
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
