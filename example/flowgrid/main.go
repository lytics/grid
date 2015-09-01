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
	flows       = flag.Int("flows", 1, "number of flows to run in parallel where each flow will have the given number of producers and consumers")
	minsize     = flag.Int("minsize", 1000, "minimum message size, actual size will be in the range [min,2*min]")
	mincount    = flag.Int("mincount", 100000, "minimum message count, actual count will be in the range [min,2*min]")
)

func main() {
	runtime.GOMAXPROCS(6)

	flag.Parse()

	etcdservers := strings.Split(*etcdconnect, ",")
	natsservers := strings.Split(*natsconnect, ",")

	conf := &Conf{
		GridName:    "flowgrid",
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
	defer j.Exit()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-exit:
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

	for i := 0; i < *flows; i++ {
		flow := NewFlow(i)

		rp := ring.New(flow.NewFlowName("producer"), conf.NrProducers, g)
		for _, def := range rp.ActorDefs() {
			def.DefineType("producer")
			def.Define("flow", flow.Name())
			err := g.StartActor(def)
			if err != nil {
				log.Fatalf("error: failed to start: %v, due to: %v", def, err)
			}
		}

		rc := ring.New(flow.NewFlowName("consumer"), conf.NrConsumers, g)
		for _, def := range rc.ActorDefs() {
			def.DefineType("consumer")
			def.Define("flow", flow.Name())
			err := g.StartActor(def)
			if err != nil {
				log.Fatalf("error: failed to start: %v, due to: %v", def, err)
			}
		}

		def := grid.NewActorDef(flow.NewFlowName("leader"))
		def.DefineType("leader")
		def.Define("flow", flow.Name())
		err = g.StartActor(def)
		if err != nil {
			log.Fatalf("error: failed to start: %v, due to: %v", "leader", err)
		}

		time.Sleep(5 * time.Second)
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

type Flow string

func NewFlow(nr int) Flow {
	return Flow(fmt.Sprintf("flow-%v", nr))
}

func (f Flow) NewFlowName(name string) string {
	return fmt.Sprintf("%v-%v", f, name)
}

func (f Flow) Name() string {
	return string(f)
}

func (f Flow) String() string {
	return string(f)
}
