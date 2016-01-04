package testutil

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/lytics/grid"
)

const TestTopic = "test-topic"

func TestTopicWriteRead(t *testing.T) {
	topic := newTopic(TestTopic, 4, 100)

	var err error
	var data []byte
	var offset int64

	// Write message "0"
	offset, err = topic.write(0, []byte("hello0"))
	if err != nil {
		t.Fatalf("topic: %v: failed to write: %v", TestTopic, err)
	}
	// Expect the offset written to == 0
	if offset != 0 {
		t.Fatalf("topic: %v: failed to increment offset: %v", TestTopic, offset)
	}
	// Expect the "HEAD" of the topic to be at offset == 1
	offset, err = topic.latest(TestTopic, 0)
	if err != nil {
		t.Fatalf("topic: %v: failed to return latest offset: %v", TestTopic, err)
	}
	if offset != 1 {
		t.Fatalf("topic: %v: failed to return expected latest offset: %v", TestTopic, offset)
	}

	// Write message "1"
	offset, err = topic.write(0, []byte("hello1"))
	if err != nil {
		t.Fatalf("topic: %v: failed to write: %v", TestTopic, err)
	}
	// Expect the offset written to == 1
	if offset != 1 {
		t.Fatalf("topic: %v: failed to increment offset: %v", TestTopic, offset)
	}
	// Expect the "HEAD" of the topic to now be at offset == 2
	offset, err = topic.latest(TestTopic, 0)
	if err != nil {
		t.Fatalf("topic: %v: failed to return latest offset: %v", TestTopic, err)
	}
	if offset != 2 {
		t.Fatalf("topic: %v: failed to return expected latest offset: %v", TestTopic, offset)
	}

	// Read at offset 0
	data, err = topic.read(0, 0)
	if err != nil {
		t.Fatalf("topic: %v: failed to read: %v", TestTopic, err)
	}
	// Expect the 0 message
	if string(data) != "hello0" {
		t.Fatalf("topic: %v: failed to read expected data: %v", TestTopic, string(data))
	}

	// Read at offset 1
	data, err = topic.read(0, 1)
	if err != nil {
		t.Fatalf("topic: %v: failed to read: %v", TestTopic, err)
	}
	// Expect the 1 message
	if string(data) != "hello1" {
		t.Fatalf("topic: %v: failed to read expected data: %v", TestTopic, string(data))
	}
}

func TestLogWriteRead(t *testing.T) {
	ml := NewMemLog()
	ml.AddDecoder(NewStuffDecoder, TestTopic)
	ml.AddEncoder(NewStuffEncoder, TestTopic)

	missing := make(map[string]bool)
	for i := 0; i < 100; i++ {
		missing[fmt.Sprintf("hello%d", i)] = true
	}

	// Write 100 messages.
	out := make(chan grid.Event)
	go func() {
		defer close(out)
		for i := 0; i < 100; i++ {
			key, msg := NewStuff(fmt.Sprintf("hello%d", i))
			out <- grid.NewWritable(TestTopic, key, msg)
		}
		ml.(*memlog).PrintTopic(TestTopic)
	}()
	ml.Write(TestTopic, out)

	// Get the available partitions.
	parts, err := ml.Partitions(TestTopic)
	if err != nil {
		t.Fatalf("topic: %v: failed to get partitions", err)
	}

	exit := make(chan bool)
	offsets := make([]int64, len(parts))

	// Read 100 messages.
	in := ml.Read(TestTopic, parts, offsets, exit)
	for event := range in {
		switch msg := event.Message().(type) {
		case *Stuff:
			delete(missing, msg.Data)
			if msg.Data == "hello99" {
				close(exit)
			}
		default:
			log.Printf("unknown message type: %T :: %v", msg, msg)
		}
	}

	if len(missing) != 0 {
		t.Fatalf("failed to find all expected written messages")
	}
}

type Stuff struct {
	Data string
}

func NewStuff(data string) ([]byte, *Stuff) {
	key := []byte(data)
	msg := &Stuff{Data: data}

	return key, msg
}

type coder struct {
	*gob.Encoder
	*gob.Decoder
}

func (c *coder) New() interface{} {
	return &Stuff{}
}

func NewStuffDecoder(r io.Reader) grid.Decoder {
	return &coder{nil, gob.NewDecoder(r)}
}

func NewStuffEncoder(w io.Writer) grid.Encoder {
	return &coder{gob.NewEncoder(w), nil}
}
