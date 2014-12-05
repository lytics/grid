package grid

import (
	"encoding/json"
	"flag"
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

	config := &KafkaConfig{BaseName: ClientName, Brokers: []string{"localhost:10092"}, ClientConfig: sarama.NewClientConfig()}
	rwlog, err := NewKafkaReadWriteLog(ClientName, config)
	if err != nil {
		t.Fatalf("Failed to create kafka log readwriter: %v", err)
	}
	rwlog.AddEncoder(newTestMesgEncoder, TopicName)

	in := make(chan Event, 0)
	rwlog.Write(TopicName, in)

	for i := 0; i < 10; i++ {
		in <- NewWritable(TopicName, "", newTestMesg(i))
	}
}

func TestReader(t *testing.T) {
	if !integrationEnabled() {
		return
	}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	config := &KafkaConfig{
		Brokers:        []string{"localhost:10092"},
		BaseName:       ClientName,
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
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

	cnt := 0
	expcnt := 10
	go func(cnt *int) {
		for e := range rwlog.Read(TopicName, parts) {
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
		in <- NewWritable(TopicName, "", newTestMesg(i))
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
