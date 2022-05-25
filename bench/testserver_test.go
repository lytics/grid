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

	"github.com/lytics/grid/v3"
	"github.com/lytics/grid/v3/testetcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const mailboxName = "pingpong-leader"

type pingPongProtoActor struct {
	server *grid.Server
	wg     *sync.WaitGroup
}

func (a *pingPongProtoActor) Act(ctx context.Context) {
	logger := log.New(os.Stderr, "pingpong-actor :: ", log.LstdFlags|log.Lshortfile)
	// Listen to a mailbox with the same
	// name as the actor.
	mailbox, err := a.server.NewMailbox(mailboxName, 10)
	successOrDie(logger, err)
	defer mailbox.Close()

	a.wg.Done() // release barrier so that the testcase can begin testing.

	for {
		select {
		case <-ctx.Done():
			return
		case req, closed := <-mailbox.C():
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
}

func runPingPongGrid(t testing.TB) (*grid.Server, *grid.Client) {
	namespace := fmt.Sprintf("bench-pingpong-namespace-%d", rand.Int63())
	logger := log.New(os.Stderr, namespace+": ", log.LstdFlags|log.Lshortfile)

	embed := testetcd.NewEmbedded(t)

	cfg := clientv3.Config{
		Endpoints:   []string{embed.Cfg.ACUrls[0].String()},
		DialTimeout: time.Second,
	}
	etcd, err := clientv3.New(cfg)
	if err != nil {
		t.Fatalf("creating etcd client: %v", err)
	}
	t.Cleanup(func() {
		if err := etcd.Close(); err != nil {
			t.Fatalf("closing etcd client: %v", err)
		}
	})

	server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: namespace})
	if err != nil {
		t.Fatalf("creating Grid server: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	server.RegisterDef("leader", func(_ []byte) (grid.Actor, error) {
		return &pingPongProtoActor{server: server, wg: &wg}, nil
	})

	err = grid.Register(Event{})
	if err != nil {
		t.Fatalf("registering Event: %v", err)
	}
	err = grid.Register(EventResponse{})
	if err != nil {
		t.Fatalf("registering EventResponse: %v", err)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("creating listener: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(lis); err != nil {
			// NOTE (2022-05) (mh): Should we let the test capture this error?
			t.Logf("error running Grid server: %v", err)
		}
	}()
	t.Cleanup(func() { <-done })
	t.Cleanup(server.Stop)

	client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: namespace, Logger: logger})
	if err != nil {
		t.Fatalf("creating Grid client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("closing client: %v", err)
		}
	})

	wg.Wait()

	return server, client
}

func successOrDie(logger *log.Logger, err error) {
	if err != nil {
		logger.Println("exiting due to error:", err)
		time.Sleep(200 * time.Millisecond)
		os.Exit(-4)
	}
}
