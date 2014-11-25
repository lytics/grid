package grid

import (
	"bytes"
	"encoding/gob"
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
	name      string
	pconfig   *sarama.ProducerConfig
	cconfig   *sarama.ConsumerConfig
	consumers []*sarama.Consumer
	producers []*sarama.Producer
	ops       map[string]*op
	wg        *sync.WaitGroup
	mutex     *sync.Mutex
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

	g := &Grid{
		kafka:     kafka,
		name:      name,
		pconfig:   pconfig,
		cconfig:   cconfig,
		consumers: make([]*sarama.Consumer, 0),
		ops:       make(map[string]*op),
		wg:        new(sync.WaitGroup),
		mutex:     new(sync.Mutex),
	}

	g.wg.Add(1)

	return g, nil
}

func (g *Grid) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	go func() {
		out := make(chan *VoterMesg)
		defer close(out)

		in, err := g.CtrlTopic(out)
		if err != nil {
			return
		}

		voter(0, 1, 0, in, out)
	}()

	for fname, op := range g.ops {
		log.Printf("grid: starting %v => %v()", op.topic, fname)
		in, err := g.reader(fname, op.topic)
		if err != nil {
			return fmt.Errorf("grid: %v(): failed to start input: %v", fname, err)
		}

		go func(g *Grid, in <-chan Mesg, f func(<-chan Mesg) <-chan Mesg) {
			g.writer(f(in))
		}(g, in, op.f)
	}
	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.consumers == nil {
		return
	}

	log.Printf("grid: shutting down")
	for _, consumer := range g.consumers {
		consumer.Close()
	}
	g.consumers = nil
	g.wg.Done()
}

func (g *Grid) Add(n int, f func(in <-chan Mesg) <-chan Mesg, topic string) error {
	fname := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	} else {
		g.ops[fname] = &op{f: f, n: n, topic: topic}
		return nil
	}
}

func (g *Grid) CtrlTopic(in <-chan *VoterMesg) (<-chan *VoterMesg, error) {
	topic := fmt.Sprintf("%v-ctrl", g.name)

	// Channels that represent Kafka, which take values of
	// type []byte.
	kout := make(chan Mesg)
	kin, err := g.reader("voter", topic)
	if err != nil {
		return nil, err
	}
	err = g.writer(kout)
	if err != nil {
		return nil, err
	}

	// Channels that pass VoterMesg(s).
	out := make(chan *VoterMesg)

	// Read data from Kafka, by transforming messages with a
	// []byte value from the kafka-in channel, to type
	// VoterMesg(s).
	go func() {
		defer close(out)

		var buf bytes.Buffer
		dec := gob.NewDecoder(&buf)

		for m := range kin {
			buf.Write(m.Value())
			vm := &VoterMesg{}
			err := dec.Decode(vm)
			if err != nil {
				log.Printf("error: grid: voter decoding: %v", err)
			}
			out <- vm
		}
	}()

	// Write data to Kafka, by transforming VoterMesg(s) to
	// messages with a []byte value and write them to the
	// kafka-out channel.
	go func() {
		defer close(kout)

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		for vm := range in {
			err := enc.Encode(vm)
			if err != nil {
				log.Printf("error: grid: voter encoding: %v", err)
			}
			val := make([]byte, buf.Len())
			buf.Read(val)
			kout <- &mesg{header{topic: topic}, nil, val}
		}
	}()

	return out, nil
}

func (g *Grid) reader(fname string, topic string) (<-chan Mesg, error) {

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
		go func(g *Grid, wg *sync.WaitGroup, part int32, out chan<- Mesg) {
			defer wg.Done()

			consumer, err := g.readfrom(fname, topic, part)
			if err != nil {
				return
			}

			for event := range consumer.Events() {
				if event.Err != nil {
					log.Printf("error: grid: %v: topic: %v: partition: %v: %v", g.name, topic, part, event.Err)
					g.Stop()
				}
				out <- &mesg{header{event.Topic, event.Partition, event.Offset, event.Err}, event.Key, event.Value}
			}
		}(g, wg, part, out)
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

func (g *Grid) readfrom(fname, topic string, part int32) (*sarama.Consumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("grid: topic name cannot be empty")
	}
	if part < 0 {
		return nil, fmt.Errorf("grid: partition number cannnot be negative")
	}

	group := fmt.Sprintf("%v-%v-%v", g.name, fname, topic)
	consumer, err := sarama.NewConsumer(g.kafka, topic, part, group, g.cconfig)
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
