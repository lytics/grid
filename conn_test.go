package grid

import (
	"testing"
	"time"

	"github.com/lytics/grid/natsunit"
	"github.com/nats-io/nats"
)

func TestSenderTimeouts(t *testing.T) {
	server, err := natsunit.StartEmbeddedNATS()
	if err != nil {
		t.Fatalf("error starting embedded NATS: %v", err)
	}
	defer server.Shutdown()

	nc, err := nats.Connect(natsunit.TestURL)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer nc.Close()

	ec, err := nats.NewEncodedConn(nc, nats.GOB_ENCODER)
	if err != nil {
		t.Fatalf("error creating new conn: %v", err)
	}
	defer ec.Close()

	sender, err := NewSender(ec, 10)
	if err != nil {
		t.Fatalf("error creating sender: %v", err)
	}
	defer sender.Close()

	timeout := time.Second
	SetConnSendTimeout(sender, timeout)

	receiver, err := NewReceiver(ec, "receiver", 1, 0)
	if err != nil {
		t.Fatalf("error creating rx: %v", err)
	}
	defer receiver.Close()

	done := make(chan bool)
	expected := errFailedToSend(defaultSendRetries, defaultSendRetries*timeout)
	go func() {
		defer close(done)
		for {
			if err := sender.Send("receiver", []byte("lol")); err != nil {
				if expected.Error() != err.Error() {
					t.Fatalf("unexpected error: %v", err)
				}
				// we got the expected error
				return
			}
		}
	}()

	select {
	case <-time.After(2 * defaultSendRetries * timeout):
		t.Fatalf("test timed out")
	case <-done:
	}
}
