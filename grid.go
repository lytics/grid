package grid

import (
	"fmt"
	"io"
	"log"
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
	log           ReadWriteLog
	gridname      string
	cmdtopic      string
	npeers        int
	quorum        uint32
	maxleadertime int64
	parts         map[string][]int32
	ops           map[string]*op
	wg            *sync.WaitGroup
	exit          chan bool
}

func New(gridname string, npeers int) (*Grid, error) {

	brokers := []string{"localhost:10092"}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	kafkaClientConfig := &KafkaConfig{
		Brokers:        brokers,
		BaseName:       gridname,
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
	}

	return NewWithKafkaConfig(gridname, npeers, kafkaClientConfig)
}

func NewWithKafkaConfig(gridname string, npeers int, kconfig *KafkaConfig) (*Grid, error) {
	cmdtopic := gridname + "-cmd"

	rwlog, err := NewKafkaReadWriteLog(buildPeerName(0), kconfig)
	if err != nil {
		return nil, err
	}

	g := &Grid{
		log:      rwlog,
		gridname: gridname,
		cmdtopic: cmdtopic,
		npeers:   npeers,
		quorum:   uint32((npeers / 2) + 1),
		parts:    make(map[string][]int32),
		ops:      make(map[string]*op),
		wg:       new(sync.WaitGroup),
		exit:     make(chan bool),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, cmdtopic)
	g.AddEncoder(NewCmdMesgEncoder, cmdtopic)

	return g, nil
}

func (g *Grid) Start() error {
	cmdpart := []int32{0}

	voter := NewVoter(0, g)
	manager := NewManager(0, g)

	// Why are these both read-only channels?
	// Because:
	//  * The readers output is our input.
	//  * Our output is the writers input.
	var in <-chan Event
	var out <-chan Event

	in = g.log.Read(g.cmdtopic, cmdpart)
	out = voter.startStateMachine(in)
	g.log.Write(g.cmdtopic, out)

	in = g.log.Read(g.cmdtopic, cmdpart)
	out = manager.startStateMachine(in)
	g.log.Write(g.cmdtopic, out)

	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	close(g.exit)
	g.wg.Done()
}

func (g *Grid) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	g.log.AddDecoder(makeDecoder, topics...)
}

func (g *Grid) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	g.log.AddEncoder(makeEncoder, topics...)
}

func (g *Grid) Add(fname string, n int, f func(in <-chan Event) <-chan Event, topics ...string) error {
	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	}

	op := &op{f: f, n: n, inputs: make(map[string]bool)}

	for _, topic := range topics {
		if _, found := g.log.DecodedTopics()[topic]; !found {
			return fmt.Errorf("grid: no decoder added for topic: %v", topic)
		}

		parts, err := g.log.Partitions(topic)
		if err != nil {
			log.Fatalf("error: topic: %v: failed getting kafka partition data: %v", g.cmdtopic, err)
		}
		g.parts[topic] = parts

		op.inputs[topic] = true
	}

	g.ops[fname] = op

	return nil
}

func (g *Grid) starti(inst *Instance) {
	fname := inst.fname

	if _, exists := g.ops[fname]; !exists {
		log.Fatalf("error: grid: does not exist: %v(): reader of: %v", fname)
	}

	ins := make([]<-chan Event, 0)
	for topic, parts := range inst.topicslices {
		if !g.ops[fname].inputs[topic] {
			log.Fatalf("error: grid: %v(): not set as reader of: %v", fname, topic)
		}

		ins = append(ins, g.log.Read(topic, parts))
	}

	out := g.ops[fname].f(merge(ins))

	outs := make(map[string]chan Event)
	for topic, _ := range g.log.EncodedTopics() {
		outs[topic] = make(chan Event, 1024)
		g.log.Write(topic, outs[topic])

		go func(fname string, out <-chan Event, outs map[string]chan Event) {
			for event := range out {
				if topicout, found := outs[event.Topic()]; found {
					topicout <- event
				} else {
					log.Printf("error: grid: %v(): has no encoder as writer of: %v", fname, event.Topic())
				}
			}
		}(fname, out, outs)
	}
}

type op struct {
	n      int
	f      func(in <-chan Event) <-chan Event
	inputs map[string]bool
}

func merge(ins []<-chan Event) <-chan Event {
	merged := make(chan Event, 1024)
	for _, in := range ins {
		go func(in <-chan Event) {
			for m := range in {
				merged <- m
			}
		}(in)
	}
	return merged
}
