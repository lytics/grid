package grid

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

const (
	ClientName = "grid-test-client"
	TopicName  = "grid-test-topic"
)

var tag = flag.String("tag", "", "integration")

func integrationEnabled() bool {
	return tag != nil && *tag == "integration"
}

func TestWriter(t *testing.T) {
	if !integrationEnabled() {
		return
	}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	config := &KafkaConfig{
		Brokers:        []string{"localhost:10092"},
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
		basename:       ClientName,
	}

	rwlog, err := NewKafkaReadWriteLog(ClientName, config)
	if err != nil {
		t.Fatalf("Failed to create kafka log readwriter: %v", err)
	}
	rwlog.AddEncoder(newTestMesgEncoder, TopicName)

	in := make(chan Event, 0)
	rwlog.Write(TopicName, in)

	for i := 0; i < 10; i++ {
		in <- NewWritable(TopicName, nil, newTestMesg(i))
	}
}

func TestReadWriter(t *testing.T) {
	if !integrationEnabled() {
		return
	}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	config := &KafkaConfig{
		Brokers:        []string{"localhost:10092"},
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
		basename:       ClientName,
	}

	rwlog, err := NewKafkaReadWriteLog(ClientName, config)
	if err != nil {
		t.Fatalf("Failed to create kafka log readwriter: %v", err)
	}
	rwlog.AddEncoder(newTestMesgEncoder, TopicName)
	rwlog.AddDecoder(newTestMesgDecoder, TopicName)

	in := make(chan Event, 0)
	rwlog.Write(TopicName, in)

	parts, err := rwlog.Partitions(TopicName)
	if err != nil {
		t.Fatalf("error: topic: %v: failed getting kafka partition data: %v", TopicName, err)
	}

	offsets := make([]int64, len(parts))
	for _, part := range parts {
		_, max, err := rwlog.Offsets(TopicName, part)
		if err != nil {
			t.Fatalf("error: topic: %v: failed getting offset for partition: %v: %v", TopicName, part, err)
		}
		offsets[part] = max
	}

	cnt := 0
	expcnt := 10
	exit := make(chan bool)
	go func(cnt *int) {
		for e := range rwlog.Read(TopicName, parts, offsets, exit) {
			switch msg := e.Message().(type) {
			case *TestMesg:
				// fmt.Printf("rx: %v\n", msg)
				if msg.Data != *cnt {
					t.Fatalf("expected message #%d to equal %d, but was: %d", *cnt, *cnt, msg.Data)
				}
			default:
				t.Fatalf("unknown message type received on: %v: %T :: %v", TopicName, msg, msg)
			}
			*cnt++
		}
	}(&cnt)

	// Removing this time-sleep will cause the writer to start
	// writing before the reader can start reading, which will
	// cause the reader to miss messages and the test will
	// fail.
	time.Sleep(2 * time.Second)

	for i := 0; i < expcnt; i++ {
		in <- NewWritable(TopicName, nil, newTestMesg(i))
	}

	// Removing this time-sleep will cause the reader to not
	// have enough time to read all messages from the topic
	// and the test will fail.
	time.Sleep(3 * time.Second)

	if cnt != 10 {
		t.Fatalf("reader did not receive expected count: %v actual count: %d", expcnt, cnt)
	}
}

type TestMesg struct {
	Data int
}

func newTestMesg(i int) *TestMesg {
	return &TestMesg{i}
}

type testcoder struct {
	*json.Encoder
	*json.Decoder
}

func (c *testcoder) New() interface{} {
	return &TestMesg{}
}

func newTestMesgDecoder(r io.Reader) Decoder {
	return &testcoder{nil, json.NewDecoder(r)}
}

func newTestMesgEncoder(w io.Writer) Encoder {
	return &testcoder{json.NewEncoder(w), nil}
}

type nooprwlog struct {
	encoded map[string]bool
	decoded map[string]bool
}

func newNoOpReadWriteLog() ReadWriteLog {
	return &nooprwlog{encoded: make(map[string]bool), decoded: make(map[string]bool)}
}

func (n *nooprwlog) Write(topic string, in <-chan Event) {
	// Do nothing.
}

func (n *nooprwlog) Read(topic string, parts []int32, offsets []int64, exit <-chan bool) <-chan Event {
	out := make(chan Event)
	go func() {
		for _ = range out {
			// Discard.
		}
	}()
	return out
}

func (n *nooprwlog) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	for _, topic := range topics {
		n.encoded[topic] = true
	}
}

func (n *nooprwlog) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	for _, topic := range topics {
		n.decoded[topic] = true
	}
}

func (n *nooprwlog) EncodedTopics() map[string]bool {
	return n.encoded
}

func (n *nooprwlog) DecodedTopics() map[string]bool {
	return n.decoded
}

func (n *nooprwlog) Partitions(topic string) ([]int32, error) {
	if n.encoded[topic] {
		return []int32{0}, nil
	}
	if n.decoded[topic] {
		return []int32{0}, nil
	}
	return nil, fmt.Errorf("no such topic: %v", topic)
}

func (n *nooprwlog) Offsets(topic string, part int32) (int64, int64, error) {
	return 0, 0, nil
}

func (n *nooprwlog) AddPartitioner(p Partitioner, topics ...string) {
	// Do nothing.
}
