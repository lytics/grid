package grid

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

type Ack struct {
	Hash int64
}

type Envelope struct {
	Hash int64
	Data []interface{}
}

type Receiver interface {
	Msgs() <-chan interface{}
	Close()
}

type receiver struct {
	ec           *nats.EncodedConn
	name         string
	exit         chan bool
	intput       chan interface{}
	stoponce     *sync.Once
	numreceivers int
}

// NewReciever creates a reciver for the given name, using subs number of go routines
// to read incoming messages with the given connection.
func NewReceiver(ec *nats.EncodedConn, name string, subs, bufsize int) (Receiver, error) {
	r := &receiver{
		ec:           ec,
		name:         name,
		exit:         make(chan bool),
		intput:       make(chan interface{}, bufsize),
		stoponce:     new(sync.Once),
		numreceivers: subs,
	}
	for i := 0; i < r.numreceivers; i++ {
		sub, err := r.ec.QueueSubscribe(r.name, r.name, func(subject, reply string, m *Envelope) {
			for _, d := range m.Data {
				select {
				case <-r.exit:
					return
				case r.intput <- d:
				}
			}
			r.ec.Publish(reply, &Ack{Hash: m.Hash})
		})
		if err != nil {
			r.Close()
			return nil, err
		}
		go func() {
			<-r.exit
			sub.Unsubscribe()
		}()
	}
	return r, nil
}

// Msgs to receive.
func (r *receiver) Msgs() <-chan interface{} {
	return r.intput
}

// Close.
func (r *receiver) Close() {
	r.stoponce.Do(func() {
		close(r.exit)
	})
}

type Sender interface {
	Send(receiver string, m interface{}) error
	SendBuffered(receiver string, m interface{}) error
	Flush() error
	Close()
}

type sender struct {
	mu               *sync.Mutex
	ec               *nats.EncodedConn
	dice             *rand.Rand
	exit             chan bool
	outputs          map[string][]interface{}
	stoponce         *sync.Once
	bufsize          int
	sendretries      int
	sendtiemout      time.Duration
	nextenvelopehash int64
}

// NewSender creates a buffered sender which can be used to send to
// an arbitrary number of receivers. Each destination receiver
// receives its own buffer.
func NewSender(ec *nats.EncodedConn, bufsize int) (Sender, error) {
	if bufsize < 0 {
		return nil, fmt.Errorf("buffer size must be positive number")
	}
	dice := NewSeededRand()
	s := &sender{
		mu:               new(sync.Mutex),
		ec:               ec,
		dice:             dice,
		exit:             make(chan bool),
		outputs:          make(map[string][]interface{}),
		stoponce:         new(sync.Once),
		bufsize:          bufsize,
		sendretries:      3,
		sendtiemout:      4 * time.Second,
		nextenvelopehash: dice.Int63(),
	}
	return s, nil
}

// Send a message to the receiver, if error is nil the message
// has been delivered.
func (s *sender) Send(receiver string, m interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.send(receiver, []interface{}{m})
}

// SendBuffered buffers the message, unless the buffer is full. If it
// is full then currently buffered messages are sent first, then the
// current message if buffered. If sending a full buffer fails a non
// nil error is returned, and the current message is not buffered.
func (s *sender) SendBuffered(receiver string, m interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, ok := s.outputs[receiver]
	if !ok {
		buf = make([]interface{}, 0, s.bufsize)
		s.outputs[receiver] = buf
	}
	if len(buf) >= s.bufsize {
		err := s.send(receiver, buf)
		if err == nil {
			buf = make([]interface{}, 0, s.bufsize)
			buf = append(buf, m)
			s.outputs[receiver] = buf
			return nil
		} else {
			return err
		}
	} else {
		buf = append(buf, m)
		s.outputs[receiver] = buf
	}
	return nil
}

// Flush forces the send of all buffered messages.
func (s *sender) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for receiver, buf := range s.outputs {
		err := s.send(receiver, buf)
		if err == nil {
			delete(s.outputs, receiver)
		} else {
			return err
		}
	}
	return nil
}

// Close.
func (s *sender) Close() {
	s.stoponce.Do(func() {
		close(s.exit)
	})
}

func (s *sender) send(receiver string, ms []interface{}) error {
	var t int
	for ; t < s.sendretries; t++ {
		ack := &Ack{}
		err := s.ec.Request(receiver, &Envelope{Data: ms, Hash: s.nextenvelopehash}, ack, s.sendtiemout)
		if err == nil && ack != nil && ack.Hash == s.nextenvelopehash {
			s.nextenvelopehash = s.dice.Int63()
			return nil
		}
		if err == nil && ack != nil && ack.Hash != s.nextenvelopehash {
			return fmt.Errorf("received bad ack from: %v, expected ack: %v, got: %v", receiver, s.nextenvelopehash, ack.Hash)
		}
		if err == nil && ack == nil {
			return fmt.Errorf("received no ack from: %v", receiver)
		}
		if err != nil && err != nats.ErrTimeout {
			return err
		}
		select {
		case <-s.exit:
			return fmt.Errorf("failed to send before exit requested")
		default:
			// Exit not wanted, try again.
		}
	}
	return fmt.Errorf("failed to send after %d attempts with total time: %s", t, time.Duration(t)*s.sendtiemout)
}

// SetConnSendTimeout changes the timeout of send operations. Setting
// this low, while at the same time using a large buffer size with
// large messages may cause sends to error due to insufficient
// time to send all data. The default is 4 seconds. Use this to
// change the setting after creating the sender if the default
// is not suitable.
func SetConnSendTimeout(s Sender, timeout time.Duration) error {
	switch s := s.(type) {
	case *sender:
		s.sendtiemout = timeout
		return nil
	}
	return fmt.Errorf("unknown sender type: %T", s)
}

// SetConnSendRetries changes the number of attempts to resend data.
// The total number of send attempts will be 1 + n. The default
// is 3 retries. Use this to change the setting after creating the
// sender if the default is not suitable.
func SetConnSendRetries(s Sender, n int) error {
	switch s := s.(type) {
	case *sender:
		if n >= 0 {
			s.sendretries = 1 + n
			return nil
		} else {
			return fmt.Errorf("number of retries must be non-negative")
		}
	}
	return fmt.Errorf("unknown sender type: %T", s)
}
