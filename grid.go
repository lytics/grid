package grid

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"sync"

	"github.com/Shopify/sarama"
)

type Decoder interface {
	New() interface{}
	Decode(d interface{}) error
}

type Encoder interface {
	Encode(e interface{}) error
}

type Grid struct {
	kafka     *sarama.Client
	name      string
	pconfig   *sarama.ProducerConfig
	cconfig   *sarama.ConsumerConfig
	consumers []*sarama.Consumer
	producers []*sarama.Producer
	encoders  map[string]func(io.Writer) Encoder
	decoders  map[string]func(io.Reader) Decoder
	ops       map[string]*op
	wg        *sync.WaitGroup
	mutex     *sync.Mutex
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
		encoders:  make(map[string]func(io.Writer) Encoder),
		decoders:  make(map[string]func(io.Reader) Decoder),
		ops:       make(map[string]*op),
		wg:        new(sync.WaitGroup),
		mutex:     new(sync.Mutex),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, fmt.Sprintf("%v-cmd", name))
	g.AddEncoder(NewCmdMesgEncoder, fmt.Sprintf("%v-cmd", name))

	return g, nil
}

func (g *Grid) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	go func() {
		out := make(chan *CmdMesg)
		defer close(out)

		in, err := g.cmdTopicChannels(out)
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

		go func(g *Grid, in <-chan Event, f func(<-chan Event) <-chan Event) {
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

func (g *Grid) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	for _, topic := range topics {
		// Only add the decoder if it has not been added before, this is
		// used to register certain decoders before the user can.
		if _, added := g.decoders[topic]; !added {
			g.decoders[topic] = makeDecoder
		}
	}
}

func (g *Grid) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	for _, topic := range topics {
		// Only add the encoder if it has not been added before, this is
		// used to register certain encoders before the user can.
		if _, added := g.encoders[topic]; !added {
			g.encoders[topic] = makeEncoder
		}
	}
}

func (g *Grid) Add(n int, f func(in <-chan Event) <-chan Event, topic string) error {
	fname := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()

	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	}

	g.ops[fname] = &op{f: f, n: n, topic: topic}
	return nil
}

func (g *Grid) cmdTopicChannels(in <-chan *CmdMesg) (<-chan *CmdMesg, error) {
	topic := fmt.Sprintf("%v-cmd", g.name)

	kout := make(chan Event)
	kin, err := g.reader("voter", topic)
	if err != nil {
		return nil, err
	}
	err = g.writer(kout)
	if err != nil {
		return nil, err
	}

	out := make(chan *CmdMesg)
	go func() {
		defer close(out)
		for e := range kin {
			out <- e.Message().(*CmdMesg)
		}
	}()

	go func() {
		defer close(kout)
		for vm := range in {
			kout <- NewWritable(topic, "", vm)
		}
	}()

	return out, nil
}

func (g *Grid) reader(fname string, topic string) (<-chan Event, error) {

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
	out := make(chan Event, 0)

	for _, part := range parts {
		go func(g *Grid, wg *sync.WaitGroup, part int32, out chan<- Event) {
			defer wg.Done()

			var buf bytes.Buffer
			var err error

			consumer, err := g.readfrom(fname, topic, part)
			if err != nil {
				return
			}

			dec := g.decoders[topic](&buf)
			for e := range consumer.Events() {
				val := dec.New()
				buf.Write(e.Value)
				err = dec.Decode(val)
				if err != nil {
					log.Printf("error: grid: decode failed: %v: value: %v", err, buf.Bytes())
					buf.Reset()
					continue
				}

				out <- NewReadable(e.Err, topic, part, e.Offset, val)
			}
		}(g, wg, part, out)
	}

	// When the kafka consumers have exited, it means there is
	// no goroutine which can write to the out channel, so
	// close it.
	go func(wg *sync.WaitGroup, out chan<- Event) {
		wg.Wait()
		close(out)
	}(wg, out)

	// The out channel is returned as a read only channel
	// so no one can close it except this code.
	return out, nil
}

func (g *Grid) writer(in <-chan Event) error {
	producer, err := sarama.NewProducer(g.kafka, g.pconfig)
	if err != nil {
		return err
	}
	go func(in <-chan Event, producer *sarama.Producer) {
		defer producer.Close()

		var buf bytes.Buffer
		var enc Encoder
		var err error

		// The producer is not tied to one topic like the consumers are
		// so an encoder is needed for ever registered topic.
		encoders := make(map[string]Encoder)
		for topic, newEncoder := range g.encoders {
			encoders[topic] = newEncoder(&buf)
		}

		for e := range in {
			enc = encoders[e.Topic()]
			err = enc.Encode(e.Message())
			if err != nil {
				log.Printf("error: grid: encode failed: %v: message: %v", err, e.Message())
				buf.Reset()
				continue
			}
			key := []byte(e.Key())
			val := make([]byte, buf.Len())
			buf.Read(val)

			producer.Input() <- &sarama.MessageToSend{Topic: e.Topic(), Key: encodable(key), Value: encodable(val)}
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
	f     func(in <-chan Event) <-chan Event
	topic string
}

// encodable is a wrapper for byte arrays to satisfy Sarama's
// crappy encoder interface. Crappy because it does not match
// the interface used by all the encoders in the standard
// packege "encoding/..."
type encodable []byte

func (e encodable) Encode() ([]byte, error) {
	return e, nil
}

func (e encodable) Length() int {
	return len(e)
}
