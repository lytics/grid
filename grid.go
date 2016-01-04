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

type Fataler func(format string, v ...interface{})

var Seppuku Fataler = DefaultFataler

func DefaultFataler(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}

type Grid struct {
	log        ReadWriteLog
	gridname   string
	cmdtopic   string
	statetopic string
	npeers     int
	quorum     uint32
	parts      map[string][]int32
	actorconfs map[string]*actorconf
	wg         *sync.WaitGroup
	exit       chan bool
	registry   metrics.Registry
	// Test hook, normaly should be 0.
	maxleadertime int64
	coordopts     *CoordOptions
}

func DefaultKafkaConfig() *KafkaConfig {
	brokers := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Producer.Flush.Messages = 10000
	config.Producer.Flush.Frequency = 200 * time.Millisecond
	config.Consumer.Fetch.Default = 1024 * 1024

	return &KafkaConfig{
		Brokers: brokers,
		Config:  config,
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

	return NewWithReadWriteLog(gridname, npeers, rwlog)
}

func NewWithReadWriteLog(gridname string, npeers int, rwlog ReadWriteLog) (*Grid, error) {
	cmdtopic := gridname + "-cmd"
	g := &Grid{
		log:        rwlog,
		gridname:   gridname,
		cmdtopic:   cmdtopic,
		npeers:     npeers,
		quorum:     uint32((npeers / 2) + 1),
		parts:      make(map[string][]int32),
		actorconfs: make(map[string]*actorconf),
		wg:         new(sync.WaitGroup),
		exit:       make(chan bool),
		coordopts:  NewCoordOptions(),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, cmdtopic)
	g.AddEncoder(NewCmdMesgEncoder, cmdtopic)

	return g, nil
}

func (g *Grid) SetCoordOptions(v *CoordOptions) {
	g.coordopts = v
}

func (g *Grid) Start() error {
	if g.registry == nil {
		g.registry = metrics.DefaultRegistry
	}

	voter := NewVoter(0, g.coordopts, g)
	manager := NewManager(0, g.coordopts, g)

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
		Seppuku("fatal: grid: topic: %v: failed to get offset for partition: %v: %v", g.cmdtopic, 0, err)
		///log.Fatalf("fatal: grid: topic: %v: this topic must be available for the grid to function", g.cmdtopic)
	}

	// Read needs to know topic, partitions, offsets, and
	// an exit channel for early exits.

	cmdpart := []int32{0}
	cmdoffset := []int64{max}

	in = g.log.Read(g.cmdtopic, cmdpart, cmdoffset, g.exit)
	out = voter.startStateMachine(in)
	g.log.Write(g.cmdtopic, out)

	in = g.log.Read(g.cmdtopic, cmdpart, cmdoffset, g.exit)
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

func (g *Grid) AddPartitioner(p Partitioner, topics ...string) {
	g.log.AddPartitioner(p, topics...)
}

// Add configures a set of actors, named the given name, with n instances set to run
// and reading from the given list of topics, where build is a function used to
// create each instance.
func (g *Grid) Add(name string, n int, build NewActor, topics ...string) error {
	if _, exists := g.actorconfs[name]; exists {
		return fmt.Errorf("actor: %v: already added", name)
	}

	actorconf := &actorconf{build: build, n: n, inputs: make(map[string]bool)}

	for _, topic := range topics {
		// Check that the actors intput topics all have decoders.
		if _, found := g.log.DecodedTopics()[topic]; !found {
			return fmt.Errorf("actor: %v: topic: %v: no decoder found for topic", name, topic)
		}
		actorconf.inputs[topic] = true

		// Discover the available partitions for the topic.
		parts, err := g.log.Partitions(topic)
		if err != nil {
			return fmt.Errorf("actor: %v: topic: %v: failed getting partition data: %v", name, topic, err)
		}
		g.parts[topic] = parts

		// Max parallelism is set by max partition count.
		if len(parts) < n && topic != g.statetopic {
			return fmt.Errorf("actor: %v: topic: %v: number of actors: %d is greater than number of partitions: %d", name, topic, n, len(parts))
		}
	}

	g.actorconfs[name] = actorconf

	return nil
}

// startInstance starts one actor instance. The basic steps of doing this:
//
//     1. Create the "in" and "state" channels.
//     2. Build the actor and call its Act method.
//     3. Negotiate offsets for input topics.
//     4. Connect its output channel to the output topics.
//
func (g *Grid) startInstance(inst *Instance) {
	name := inst.Name
	id := inst.Id

	// Validate early that the instance was added to the grid.
	if _, exists := g.actorconfs[name]; !exists {
		Seppuku("fatal: grid: %v-%d: never configured", name, id)
	}

	// Validate early that the instance has topics and partition subsets.
	for topic, _ := range inst.TopicSlices {
		if !g.actorconfs[name].inputs[topic] {
			Seppuku("fatal: grid: %v-%d: not set as reader of: %v", name, id, topic)
		}
	}

	// The in and state channel will be used by this instance to receive data.
	in := make(chan Event)
	state := make(chan Event)
	// Start the actor, and receive its out channel.
	out := g.actorconfs[name].build(name, id).Act(in, state)

	// Start the true topic reading. The messages on the input channels are
	// mux'ed and put onto a single merged input channel.
	for topic, parts := range inst.TopicSlices {
		go g.negotiateReadOffsets(id, name, topic, parts, in, state)
	}

	// Create a mapping of all output topics defined which the instance
	// may write to before connecting the instance to its out channel.
	// Each actore instance has its own map, this way no lock/unlock is
	// needed per message send.
	topics := make(map[string]chan Event)
	for topic, _ := range g.log.EncodedTopics() {
		topics[topic] = make(chan Event, 1024)
	}

	// Start the true topic writing. The messages on the output channel are
	// de-mux'ed and put on topic-specific output channels.
	for topic, _ := range g.log.EncodedTopics() {
		g.log.Write(topic, topics[topic])
		go writer(out, topics)
	}
}

// negotiateReadOffsets sends min max offset information for its topic
// to its actor. When the actor has responded with all the needed
// offset choices, reading of those topic parts is started.
func (g *Grid) negotiateReadOffsets(id int, name, topic string, parts []int32, in, state chan<- Event) {
	response := make(chan *useoffset)
	defer close(response)

	// Send the current min and max available offset for
	// any given partition for this topic. The actor
	// will need to take this into consideration when
	// choosing the offset to start reading at.
	go func() {
		for _, part := range parts {
			min, max, err := g.log.Offsets(topic, part)
			if err != nil {
				Seppuku("fatal: grid: %v-%d: topic: %v: partition: %v: failed getting offset: %v", name, id, topic, part, err)
			}
			state <- NewReadable(g.statetopic, 0, 0, NewMinMaxOffset(min, max, part, topic, response))
		}
	}()

	// Record the offsets chosen thus far for this topic.
	// Once a choice has been made for every partition
	// start the reading.
	chosen := make(map[int32]int64)
	for use := range response {
		chosen[use.Part] = use.Offset
		if len(chosen) == len(parts) {
			offsets := make([]int64, len(parts))
			for i, part := range parts {
				offsets[i] = chosen[part]
			}

			log.Printf("grid: %v-%d: topic: %v: partitions: %v: offsets: %v: starting reader", name, id, topic, parts, offsets)

			events := g.log.Read(topic, parts, offsets, g.exit)
			if topic == g.statetopic {
				go reader(state, events)
			} else {
				state <- NewReadable(g.statetopic, 0, 0, NewTopicReady(topic))
				go reader(in, events)
			}
			return
		}
	}
}

// writer de-multiplexes events on the actor's out channel, and
// places them on topic specific out channels.
func writer(out <-chan Event, topics map[string]chan Event) {
	for event := range out {
		if topic, found := topics[event.Topic()]; found {
			topic <- event
		} else {
			// The uesr called a topic that was never configured.
			Seppuku("fatal: grid: no encoder found for topic: %v", event.Topic())
		}
	}
}

// reader, which there could be many of, one per topic the actor is
// reading, multiplexes the read events onto the actor's in channel
// or state channel. The writable channel 'actor' represents one of
// those two.
func reader(actor chan<- Event, events <-chan Event) {
	for event := range events {
		actor <- event
	}
}

// actorconf is a configuration for a set actors in the grid.
type actorconf struct {
	n      int
	build  NewActor
	inputs map[string]bool
}
