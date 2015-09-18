package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
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
	msgsize     = flag.Int("msgsize", 1000, "minimum message size, actual size will be in the range [min,2*min]")
	msgcount    = flag.Int("msgcount", 100000, "number of messages each producer should send")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	etcdservers := strings.Split(*etcdconnect, ",")
	natsservers := strings.Split(*natsconnect, ",")

	conf := &Conf{
		GridName:    "flowgrid",
		MsgSize:     *msgsize,
		MsgCount:    *msgcount,
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

	w := condition.NewCountWatch(g.Etcd(), g.Name(), "hosts")
	defer w.Stop()

	started := w.WatchUntil(*nodes)
	select {
	case <-exit:
		log.Printf("Shutting down, grid exited")
		return
	case <-w.WatchError():
		log.Fatalf("error: failed to watch other hosts join: %v", err)
	case <-started:
	}

	for i := 0; i < *flows; i++ {
		flow := NewFlow(i)

		rp := ring.New(flow.NewContextualName("producer"), conf.NrProducers, g)
		for _, def := range rp.ActorDefs() {
			def.DefineType("producer")
			def.Define("flow", flow.Name())
			err := g.StartActor(def)
			if err != nil {
				log.Fatalf("error: failed to start: %v, due to: %v", def, err)
			}
		}

		rc := ring.New(flow.NewContextualName("consumer"), conf.NrConsumers, g)
		for _, def := range rc.ActorDefs() {
			def.DefineType("consumer")
			def.Define("flow", flow.Name())
			err := g.StartActor(def)
			if err != nil {
				log.Fatalf("error: failed to start: %v, due to: %v", def, err)
			}
		}

		def := grid.NewActorDef(flow.NewContextualName("leader"))
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

func (f Flow) NewContextualName(name string) string {
	return fmt.Sprintf("%v-%v", f, name)
}

func (f Flow) Name() string {
	return string(f)
}

func (f Flow) String() string {
	return string(f)
}
