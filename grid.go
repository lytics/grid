package grid

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

type Decoder interface {
	New() interface{}
	Decode(d interface{}) error
}

type Encoder interface {
	Encode(e interface{}) error
}

type Grid struct {
	log        ReadWriteLog
	gridname   string
	cmdtopic   string
	statetopic string
	npeers     int
	quorum     uint32
	parts      map[string][]int32
	ops        map[string]*op
	wg         *sync.WaitGroup
	exit       chan bool
	registry   metrics.Registry
	// Test hook, normaly should be 0.
	maxleadertime int64
}

func DefaultKafkaConfig() *KafkaConfig {
	brokers := []string{"localhost:10092"}

	pconfig := sarama.NewProducerConfig()
	pconfig.FlushMsgCount = 10000
	pconfig.FlushFrequency = 1 * time.Second
	cconfig := sarama.NewConsumerConfig()
	cconfig.DefaultFetchSize = 512000
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	return &KafkaConfig{
		Brokers:        brokers,
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
	}
}

func New(gridname string, npeers int) (*Grid, error) {
	return NewWithKafkaConfig(gridname, npeers, DefaultKafkaConfig())
}

func NewWithKafkaConfig(gridname string, npeers int, kconfig *KafkaConfig) (*Grid, error) {
	cmdtopic := gridname + "-cmd"

	kconfig.cmdtopic = cmdtopic
	kconfig.basename = gridname

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
	if g.registry == nil {
		g.registry = metrics.DefaultRegistry
	}

	voter := NewVoter(0, g)
	manager := NewManager(0, g)

	// Why are these both read-only channels?
	// Because:
	//  * The reader's output is our input.
	//  * Our output is the writer's input.
	var in <-chan Event
	var out <-chan Event

	// Command topic only uses the 0 partition
	// to make sure all communication has a
	// total ordering.

	_, max, err := g.log.Offsets(g.cmdtopic, 0)
	if err != nil {
		log.Fatalf("fatal: grid: topic: %v: failed to get offset for partition: %v: %v", g.cmdtopic, 0, err)
	}

	in = g.log.Read(g.cmdtopic, []int32{0}, []int64{max}, g.exit)
	out = voter.startStateMachine(in)
	g.log.Write(g.cmdtopic, out)

	in = g.log.Read(g.cmdtopic, []int32{0}, []int64{max}, g.exit)
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

func (g *Grid) UseStateTopic(topic string) {
	g.statetopic = topic
}

func (g *Grid) UseMetrics(registry metrics.Registry) {
	g.registry = registry
}

func (g *Grid) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	g.log.AddDecoder(makeDecoder, topics...)
}

func (g *Grid) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	g.log.AddEncoder(makeEncoder, topics...)
}

func (g *Grid) AddPartitioner(p func(key io.Reader, parts int32) int32, topics ...string) {
	g.log.AddPartitioner(p, topics...)
}

func (g *Grid) Add(fname string, n int, f func(in <-chan Event) <-chan Event, topics ...string) error {
	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	}

	op := &op{f: f, n: n, inputs: make(map[string]bool)}

	for _, topic := range topics {
		if _, found := g.log.DecodedTopics()[topic]; !found {
			return fmt.Errorf("grid: topic: %v: no decoder found for topic", topic)
		}
		op.inputs[topic] = true

		// Discover the available partitions for the topic.
		parts, err := g.log.Partitions(topic)
		if err != nil {
			return fmt.Errorf("grid: topic: %v: failed getting partition data: %v", topic, err)
		}
		g.parts[topic] = parts

		if len(parts) < n {
			return fmt.Errorf("grid: topic: %v: parallelism of function is greater than number of partitions: func: %v, parallelism: %d partitions: %d", topic, fname, n, len(parts))
		}
	}

	g.ops[fname] = op

	return nil
}

func (g *Grid) startinst(inst *Instance) {
	fname := inst.Fname
	id := inst.Id

	// Validate early that the instance was added to the grid.
	if _, exists := g.ops[fname]; !exists {
		log.Fatalf("fatal: grid: does not exist: %v()", fname)
	}

	// Validate early that the instance has valid topics and partition subsets.
	for topic, _ := range inst.TopicSlices {
		if !g.ops[fname].inputs[topic] {
			log.Fatalf("fatal: grid: %v(): not set as reader of: %v", fname, topic)
		}
	}

	// The in channel will be used by this instance to receive data, ie: its input.
	in := make(chan Event)

	// The out channel will be used by this instance to transmit data, ie: its output.
	out := g.ops[fname].f(in)

	if "" == g.statetopic {
		return
	}

	parts, err := g.log.Partitions(g.statetopic)
	if err != nil {
		log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: failed to get partition data: %v", fname, id, g.statetopic, err)
	}

	maxs := make([]int64, len(parts))
	mins := make([]int64, len(parts))
	for _, part := range parts {
		min, max, err := g.log.Offsets(g.statetopic, part)
		if err != nil {
			log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: part: %v: failed to get offset: %v", fname, id, g.statetopic, part, err)
		}
		maxs[part] = max
		mins[part] = min
	}

	var readstate bool
	for i, max := range maxs {
		if mins[i] != max {
			readstate = true
		}
	}

	if readstate {
		exit := make(chan bool)
		for event := range g.log.Read(g.statetopic, parts, mins, exit) {
			if event.Offset()+1 >= maxs[event.Part()] {
				in <- NewReadable(g.gridname, 0, 0, Ready(true))
				close(exit)
			} else {
				in <- event
			}
		}
	} else {
		in <- NewReadable(g.gridname, 0, 0, Ready(true))
	}

	cnt := 0
	topicoffsets := make(map[string]map[int32]int64)

	for topic, parts := range inst.TopicSlices {
		cnt += len(parts)
		topicoffsets[topic] = make(map[int32]int64)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go func() {
		defer wg.Done()
		for topic, parts := range inst.TopicSlices {
			for _, part := range parts {
				min, max, err := g.log.Offsets(topic, part)
				if err != nil {
					log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: partition: %v: failed getting offset: %v", fname, id, topic, part, err)
				}
				in <- NewReadable(g.gridname, 0, 0, MinMaxOffset{Topic: topic, Part: part, Min: min, Max: max})
			}
		}
	}()

	go func() {
		defer wg.Done()
		for event := range out {
			switch msg := event.Message().(type) {
			case UseOffset:
				topicoffsets[msg.Topic][msg.Part] = msg.Offset
				cnt--
			default:
			}
			if cnt == 0 {
				in <- NewReadable(g.gridname, 0, 0, Ready(true))
				return
			}
		}
	}()

	wg.Wait()

	// The messages on the input channels are mux'ed and put onto
	// a single merged input channel.
	for topic, parts := range inst.TopicSlices {
		go func() {
			offsets := make([]int64, len(parts))
			for i, part := range parts {
				if offset, found := topicoffsets[topic][part]; !found {
					log.Fatalf("fatal: grid: %v: instance: %v: failed to find offset for topic: %v: partition: %v", fname, id, topic, part)
				} else {
					offsets[i] = offset
				}
			}

			log.Printf("grid: starting: %v: instance: %v: topic: %v: partitions: %v: offsets: %v", fname, id, topic, parts, offsets)
			for event := range g.log.Read(topic, parts, offsets, g.exit) {
				in <- event
			}
		}()
	}

	// The messages on the output channel are de-mux'ed and put on
	// topic specific output channels.
	outs := make(map[string]chan Event)
	for topic, _ := range g.log.EncodedTopics() {
		outs[topic] = make(chan Event, 1024)
		g.log.Write(topic, outs[topic])
		go func() {
			for event := range out {
				if topicout, found := outs[event.Topic()]; found {
					topicout <- event
				} else {
					log.Fatalf("fatal: grid: %v(): not set as writer of: %v", fname, event.Topic())
				}
			}
		}()
	}

}

type MinMaxOffset struct {
	Topic string
	Part  int32
	Min   int64
	Max   int64
}

type UseOffset struct {
	Topic  string
	Part   int32
	Offset int64
}

type Ready bool

type op struct {
	n      int
	f      func(in <-chan Event) <-chan Event
	inputs map[string]bool
}

func merge(out chan<- Event, ins []<-chan Event) {

}
