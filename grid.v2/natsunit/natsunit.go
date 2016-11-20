package natsunit

import (
	"fmt"
	"time"

	"github.com/nats-io/gnatsd/server"
	gnatsd "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
)

// Create Test Options that don't Conflict with
// any local server 4222 port
var GnatsdTestOptions = server.Options{
	Host:   "localhost",
	Port:   9547,
	NoLog:  true,
	NoSigs: true,
}

var (
	TestURL = "nats://127.0.0.1:9547"
)

func StartEmbeddedNATS() (*server.Server, error) {
	const (
		retry = 10
		testq = "testembeddednatstopic"
	)

	// Start the server.
	s := gnatsd.RunServer(&GnatsdTestOptions)

	// Create a client connection.
	nc, err := nats.Connect(TestURL)
	if err != nil {
		s.Shutdown()
		return nil, err
	}

	// Make it an encoded connection.
	ec, err := nats.NewEncodedConn(nc, nats.GOB_ENCODER)
	if err != nil {
		s.Shutdown()
		return nil, err
	}
	defer ec.Close()

	// Subscribe to messages.
	sub, err := ec.QueueSubscribe(testq, testq, func(_, reply string, msg string) {
		if msg == "syn" {
			ec.Publish(reply, "ack")
		}
	})
	if err != nil {
		s.Shutdown()
		return nil, err
	}
	defer sub.Unsubscribe()

	// Try up to 'retry' times to send a message via the embedded
	// server, and return success if the ack is received.
	var ack string
	for i := 0; i < retry; i++ {
		ec.Request(testq, "syn", &ack, 10*time.Millisecond)
		if ack == "ack" {
			return s, nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Otherwise something is wrong, so shutdown the server and
	// return an error.
	s.Shutdown()
	return nil, fmt.Errorf("failed to verify embedded gnatsd is running")
}
