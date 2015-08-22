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
)

const (
	GridName = "msgcount"
)

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

	log.Printf("waiting 20 seconds before starting actors")
	time.Sleep(20 * time.Second)

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

func NewName(role string, part int) string {
	return fmt.Sprintf("%v.%v", role, part)
}

func ByModuloInt(role string, key int, n int) string {
	part := key % n
	return fmt.Sprintf("%s.%s.%d", GridName, role, part)
}

func ByNumber(role string, part int) string {
	return fmt.Sprintf("%s.%s.%d", GridName, role, part)
}
