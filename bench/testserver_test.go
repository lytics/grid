package bench

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/testetcd"
	"go.etcd.io/etcd/clientv3"
)

var (
	etcdEndpoints []string
	server        *grid.Server
	client        *grid.Client
	wg            *sync.WaitGroup
)

const mailboxNamePrefix = "pingpong-worker"
const workers = 16

func TestMain(m *testing.M) {
	embed := testetcd.NewEmbedded()
	defer embed.Etcd.Close()
	etcdEndpoints = []string{embed.Cfg.ACUrls[0].String()}
	time.Sleep(10 * time.Millisecond) // Give the test main, time to start it's etcd server
	serverT, clientT, cleanup, wgT := runPingPongGrid()
	server = serverT
	client = clientT
	wg = wgT
	time.Sleep(50 * time.Millisecond)
	r := m.Run()
	time.Sleep(10 * time.Millisecond)
	server.Stop()
	time.Sleep(10 * time.Millisecond)
	cleanup()
	time.Sleep(10 * time.Millisecond)
	os.Exit(r)
}

type pingPongProtoActor struct {
	server *grid.Server
	wg     *sync.WaitGroup
}

func (a *pingPongProtoActor) Act(ctx context.Context) {
	logger := log.New(os.Stderr, "pingpong-actor :: ", log.LstdFlags|log.Lshortfile)
	// Listen to a mailbox with the same
	// name as the actor.

	wgStarted := &sync.WaitGroup{}
	wgStarted.Add(workers)

	wgDone := &sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wgDone.Add(1)
		go func(ii int) {
			mailboxName := fmt.Sprintf("%s-%d", mailboxNamePrefix, ii)
			mailbox, err := grid.NewMailbox(a.server, mailboxName, 10)
			successOrDie(logger, err)
			defer mailbox.Close()

			wgStarted.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case req, closed := <-mailbox.C:
					if !closed {
						logger.Printf(" closed == true ")
						return
					}
					msg := req.Msg()
					switch m := msg.(type) {
					case *Event:
						var responseMsg *EventResponse
						switch m.Type {
						case SmallMsg:
							responseMsg = responseSmallMsg
						case BigStrMsg:
							responseMsg = responseBigStrMsg
						case BigMapBigStr:
							responseMsg = responseBigMapBigStrMsg
						case SuperBigStr:
							responseMsg = responseSuperBigStrMsg
						default:
							successOrDie(logger, fmt.Errorf("ERROR:  unknown message.Type %#v", m.Type))
						}
						err := req.Respond(responseMsg)
						if err != nil {
							logger.Printf("ERROR: error on message response %v\n", err)
							successOrDie(logger, err)
						}
					default:
						successOrDie(logger, fmt.Errorf("ERROR:  wrong type %#v", req.Msg()))
					}
				}
			}
		}(i)
	}

	wgStarted.Wait() // wait for the workers to startup.
	a.wg.Done()      // release barrier so that the testcase can begin testing.
	wgDone.Wait()    // wait until all the works exit
}

func runPingPongGrid() (*grid.Server, *grid.Client, func(), *sync.WaitGroup) {
	const (
		timeout = 20 * time.Second
	)

	namespace := fmt.Sprintf("bench-pingpong-namespace-%d", rand.Int63())
	logger := log.New(os.Stderr, namespace+": ", log.LstdFlags|log.Lshortfile)

	cfg := clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: time.Second,
	}
	etcd, err := clientv3.New(cfg)
	successOrDie(logger, err)

	server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: namespace})
	successOrDie(logger, err)

	client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: namespace, Logger: logger})
	successOrDie(logger, err)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	server.RegisterDef("leader", func(_ []byte) (grid.Actor, error) {
		return &pingPongProtoActor{server: server, wg: wg}, nil
	})

	err = grid.Register(Event{})
	successOrDie(logger, err)
	err = grid.Register(EventResponse{})
	successOrDie(logger, err)

	lis, err := net.Listen("tcp", "localhost:0")
	successOrDie(logger, err)

	done := make(chan error, 1)
	go func() {
		defer close(done)
		err := server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()

	return server, client, func() {
		etcd.Close()
		client.Close()
	}, wg
}

func successOrDie(logger *log.Logger, err error) {
	if err != nil {
		panic(err.Error())
		logger.Println("exiting due to error:", err)
		time.Sleep(200 * time.Millisecond)
		os.Exit(-4)
	}
}
