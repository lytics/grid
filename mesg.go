package grid

type Event interface {
	Offset() int64
	Topic() string
	Key() string
	Message() interface{}
}

type event struct {
	offset  int64
	topic   string
	key     string
	message interface{}
}

func (e *event) Offset() int64 {
	return e.offset
}

func (e *event) Topic() string {
	return e.topic
}

func (e *event) Key() string {
	return e.key
}

func (e *event) Message() interface{} {
	return e.message
}

func NewReadable(topic string, offset int64, message interface{}) Event {
	return &event{topic: topic, offset: offset, message: message}
}

func NewWritable(topic, key string, message interface{}) Event {
	return &event{topic: topic, key: key, message: message}
}
