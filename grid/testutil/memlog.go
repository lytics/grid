package testutil

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
	"time"

	"github.com/lytics/grid"
)

const nparts = 4

// memlog is an in memory log structure of topics.
type memlog struct {
	lock         *sync.Mutex
	topics       map[string]*topic                       // By topic.
	encoders     map[string]func(io.Writer) grid.Encoder // By topic.
	decoders     map[string]func(io.Reader) grid.Decoder // By topic.
	partitioners map[string]grid.Partitioner             // By topic.
	writewatch   func(grid.Event)
}

func NewMemLogWithWatch(f func(grid.Event)) grid.ReadWriteLog {
	return &memlog{
		lock:         new(sync.Mutex),
		topics:       make(map[string]*topic),
		encoders:     make(map[string]func(io.Writer) grid.Encoder),
		decoders:     make(map[string]func(io.Reader) grid.Decoder),
		partitioners: make(map[string]grid.Partitioner),
		writewatch:   f,
	}
}

func NewMemLog() grid.ReadWriteLog {
	return NewMemLogWithWatch(nil)
}

func (m *memlog) PrintTopic(topic string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, found := m.topics[topic]
	if !found {
		return fmt.Errorf("topic: %v: failed to find topic", topic)
	}

	parts, err := m.Partitions(topic)
	if err != nil {
		return fmt.Errorf("topic: %v: failed to find partitions: %v", topic, err)
	}

	var max int64
	for _, part := range parts {
		head, _ := t.latest(topic, part)
		if max < head {
			max = head
		}
	}

	var offset int64
	for ; offset < max; offset++ {
		for _, part := range parts {
			data, err := t.read(part, offset)
			if err == nil && data != nil {
				fmt.Printf("%v:%d:%5d == > %v\n", topic, part, offset, len(data))
			}
		}
	}

	return nil
}

func (m *memlog) AddPartitioner(p grid.Partitioner, topics ...string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, topic := range topics {
		m.partitioners[topic] = p
	}
}

func (m *memlog) AddEncoder(makeEncoder func(io.Writer) grid.Encoder, topics ...string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, topic := range topics {
		m.encoders[topic] = makeEncoder
	}
}

func (m *memlog) AddDecoder(makeDecoder func(io.Reader) grid.Decoder, topics ...string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, topic := range topics {
		m.decoders[topic] = makeDecoder
	}
}

func (m *memlog) EncodedTopics() map[string]bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	ts := make(map[string]bool)
	for t, _ := range m.encoders {
		ts[t] = true
	}
	return ts
}

func (m *memlog) DecodedTopics() map[string]bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	ts := make(map[string]bool)
	for t, _ := range m.decoders {
		ts[t] = true
	}
	return ts
}

func (m *memlog) Partitions(topic string) ([]int32, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	ps := make([]int32, nparts)
	for i := 0; i < nparts; i++ {
		ps[i] = int32(i)
	}
	return ps, nil
}

func (m *memlog) Offsets(topic string, part int32) (int64, int64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	tval, found := m.topics[topic]
	if !found {
		tval = newTopic(topic, nparts, 1000000)
		m.topics[topic] = tval
	}
	max, err := tval.latest(topic, part)
	if err != nil {
		return 0, 0, err
	}

	return 0, max, nil
}

func (m *memlog) Write(topic string, in <-chan grid.Event) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, found := m.topics[topic]
	if !found {
		t = newTopic(topic, nparts, 1000000)
		m.topics[topic] = t
	}

	p, found := m.partitioners[topic]
	if !found {
		p = newPartitioner()
		m.partitioners[topic] = p
	}

	e, found := m.encoders[topic]
	if !found {
		panic(fmt.Sprintf("topic: %v: no encoder found", topic))
	}

	go func() {
		for event := range in {
			if m.writewatch != nil {
				m.writewatch(event)
			}
			var buf bytes.Buffer
			enc := e(&buf)
			err := enc.Encode(event.Message())
			if err == nil {
				part := p.Partition(event.Key(), nparts)
				_, err := t.write(part, buf.Bytes())
				if err != nil {
					panic(fmt.Sprintf("topic: %v: partition: %v: error writing: %v", topic, part, err))
				}
			} else {
				panic(fmt.Sprintf("topic: %v: failed to encode: %v", t.name, err))
			}
		}
	}()
}

func (m *memlog) Read(topic string, parts []int32, offsets []int64, exit <-chan bool) <-chan grid.Event {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, found := m.topics[topic]
	if !found {
		t = newTopic(topic, nparts, 1000000)
		m.topics[topic] = t
	}

	d, found := m.decoders[topic]
	if !found {
		panic(fmt.Sprintf("topic: %v: no decoder found", topic))
	}

	out := make(chan grid.Event)
	go func() {
		defer close(out)

		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		pos := make([]int64, len(offsets))
		for i, o := range offsets {
			pos[i] = o
		}

		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				for i, p := range parts {
					data, err := t.read(p, pos[i])
					if err != nil {
						panic(fmt.Sprintf("topic: %v: partitions: %v: error reading: %v", topic, parts, err))
					}
					if data != nil {
						buf := bytes.NewBuffer(data)
						dec := d(buf)
						obj := dec.New()
						err := dec.Decode(obj)
						if err != nil {
							out <- grid.NewReadable(t.name, p, int64(pos[i]), fmt.Errorf("topic: %v: partition: %v: failed to decode: %v", t.name, p, err))
						} else {
							out <- grid.NewReadable(t.name, p, int64(pos[i]), obj)
						}
						pos[i]++
					}
				}
			}
		}
	}()
	return out
}

// topic is an in memory representation of a partitioned topic.
type topic struct {
	name string
	head []int64       // By partition number.
	lock []*sync.Mutex // By partition number.
	data [][][]byte    // By partition number, offset, message byte.
}

func newTopic(name string, nparts int, maxsize int) *topic {
	head := make([]int64, nparts)
	lock := make([]*sync.Mutex, nparts)
	data := make([][][]byte, nparts)
	for i := 0; i < nparts; i++ {
		head[i] = 0
		lock[i] = new(sync.Mutex)
		data[i] = make([][]byte, maxsize)
	}
	return &topic{name: name, head: head, lock: lock, data: data}
}

func (t *topic) latest(topic string, part int32) (int64, error) {
	if part < 0 || len(t.data) < int(part) {
		return 0, fmt.Errorf("topic: %v: partition: %v: no such partition", t.name, part)
	}

	t.lock[part].Lock()
	defer t.lock[part].Unlock()

	return t.head[part], nil
}

func (t *topic) write(part int32, data []byte) (int64, error) {
	if part < 0 || len(t.data) < int(part) {
		return 0, fmt.Errorf("topic: %v: partition: %v: no such partition", t.name, part)
	}

	t.lock[part].Lock()
	defer t.lock[part].Unlock()

	o := t.head[part]
	d := make([]byte, len(data))
	copy(d, data)
	t.data[part][o] = d
	t.head[part]++

	return o, nil
}

func (t *topic) read(part int32, offset int64) ([]byte, error) {
	if part < 0 || len(t.data) < int(part) {
		return nil, fmt.Errorf("topic: %v: partition: %v: no such partition", t.name, part)
	}

	t.lock[part].Lock()
	defer t.lock[part].Unlock()

	if t.head[part] <= offset {
		return nil, nil
	}

	d := make([]byte, len(t.data[part][offset]))
	copy(d, t.data[part][offset])

	return d, nil
}

type partitioner struct{}

func newPartitioner() grid.Partitioner {
	return &partitioner{}
}

func (p *partitioner) Partition(key []byte, nparts int32) int32 {
	if key == nil || len(key) == 0 {
		return 0
	}
	hash := fnv.New64()
	hash.Write(key)
	part := hash.Sum64() % uint64(nparts)
	return int32(part)
}
