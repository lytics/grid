package grid

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"

	"github.com/Shopify/sarama"
)

type Header interface {
	Topic() string
	Partition() int32
	Offset() int64
	Err() error
}

type Mesg interface {
	Header
	Key() []byte
	Value() []byte
}

type Grid struct {
	kafka     *sarama.Client
	group     string
	pconfig   *sarama.ProducerConfig
	cconfig   *sarama.ConsumerConfig
	consumers []*sarama.Consumer
	producers []*sarama.Producer
	ops       map[string]*op
	wg        *sync.WaitGroup
}

func NewHeader(topic string) Header {
	return header{topic: topic}
}

func New(name string) (*Grid, error) {
	kafka, err := sarama.NewClient(name, []string{"localhost:10092"}, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	grid := &Grid{
		kafka:     kafka,
		group:     name,
		pconfig:   pconfig,
		cconfig:   cconfig,
		consumers: make([]*sarama.Consumer, 0),
		ops:       make(map[string]*op),
		wg:        new(sync.WaitGroup),
	}

	return grid, nil
}

func (g *Grid) Start() error {
	g.wg.Add(1)
	for fname, op := range g.ops {
		if op.topic == "" {
			return fmt.Errorf("grid: %v(): missing input topic", fname)
		}

		log.Printf("grid: starting %v()", fname)
		in, err := g.reader(op.topic)
		if err != nil {
			return fmt.Errorf("grid: %v(): failed to start input: %v", err)
		}

		go func(g *Grid, in <-chan Mesg) {
			g.writer(op.f(in))
		}(g, in)
	}
	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	log.Printf("grid: shutting down")
	for _, consumer := range g.consumers {
		consumer.Close()
	}
	g.wg.Done()
}

func (g *Grid) Of(n int, f func(in <-chan Mesg) <-chan Mesg, topic string) error {
	fname := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: function already added")
	} else {
		g.ops[fname] = &op{f: f, n: n, topic: topic}
		return nil
	}
}

func (g *Grid) reader(topic string) (<-chan Mesg, error) {

	parts, err := g.kafka.Partitions(topic)
	if err != nil {
		return nil, err
	}

	// Setup a wait group so that the out channel
	// can be closed when all consumers have
	// exited.
	wg := new(sync.WaitGroup)
	wg.Add(len(parts))

	// Consumers read from the real topic and push data
	// into the out channel.
	out := make(chan Mesg, 0)

	for _, part := range parts {
		go func(g *Grid, part int32, out chan<- Mesg) {
			defer wg.Done()

			consumer, err := g.consumer(topic, part)
			if err != nil {
				return
			}

			for event := range consumer.Events() {
				if event.Err != nil {
					log.Printf("error: grid: consumer: %v partition: %v: %v", g.group, part, event.Err)
					g.Stop()
				}
				out <- &mesg{header{event.Topic, event.Partition, event.Offset, event.Err}, event.Key, event.Value}
			}
		}(g, part, out)
	}

	// When the kafka consumers have exited, it means there is
	// no goroutine which can write to the out channel, so
	// close it.
	go func(wg *sync.WaitGroup, out chan<- Mesg) {
		wg.Wait()
		close(out)
	}(wg, out)

	// The out channel is returned as a read only channel
	// so no one can close it except this code.
	return out, nil
}

func (g *Grid) writer(in <-chan Mesg) error {
	producer, err := sarama.NewProducer(g.kafka, g.pconfig)
	if err != nil {
		return err
	}
	go func(in <-chan Mesg, producer *sarama.Producer) {
		defer producer.Close()
		for event := range in {
			producer.Input() <- &sarama.MessageToSend{Topic: event.Topic(), Key: encodable(event.Key()), Value: encodable(event.Value())}
		}
	}(in, producer)

	return nil
}

func (g *Grid) consumer(topic string, part int32) (*sarama.Consumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("grid: topic name cannot be empty")
	}
	if part < 0 {
		return nil, fmt.Errorf("grid: partition number cannnot be negative")
	}

	consumer, err := sarama.NewConsumer(g.kafka, topic, part, g.group, g.cconfig)
	if err != nil {
		return nil, err
	}
	g.consumers = append(g.consumers, consumer)
	return consumer, nil
}

type op struct {
	n     int
	f     func(in <-chan Mesg) <-chan Mesg
	topic string
}

type header struct {
	topic     string
	partition int32
	offset    int64
	err       error
}

func (h header) Err() error {
	return h.err
}

func (h header) Topic() string {
	return h.topic
}

func (h header) Offset() int64 {
	return h.offset
}

func (h header) Partition() int32 {
	return h.partition
}

type mesg struct {
	header
	key   []byte
	value []byte
}

func (m *mesg) Key() []byte {
	return m.key
}

func (m *mesg) Value() []byte {
	return m.value
}

type encodable []byte

func (e encodable) Encode() ([]byte, error) {
	return e, nil
}

func (e encodable) Length() int {
	return len(e)
}
