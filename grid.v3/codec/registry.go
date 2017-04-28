package codec

import (
	"errors"
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
)

var (
	ErrNonProtoMessage         = errors.New("codec: non proto message")
	ErrUnregisteredMessageType = errors.New("codec: unregistered message type")
)

var (
	mu       = &sync.RWMutex{}
	registry = map[string]interface{}{}
)

func Register(v interface{}) error {
	mu.Lock()
	defer mu.Unlock()

	// The value 'v' must not be registered
	// as a pointer type, but to check if
	// it is a proto message, the pointer
	// type must be checked.
	pv := reflect.New(reflect.TypeOf(v)).Interface()

	_, ok := pv.(proto.Message)
	if !ok {
		return ErrNonProtoMessage
	}

	name := TypeName(v)
	registry[name] = v
	return nil
}

func Marshal(v interface{}) (string, []byte, error) {
	mu.RLock()
	defer mu.RUnlock()

	name := TypeName(v)
	_, ok := registry[name]
	if !ok {
		return "", nil, ErrUnregisteredMessageType
	}
	buf, err := protoMarshal(v)
	if err != nil {
		return "", nil, err
	}
	return name, buf, nil
}

func Unmarshal(buf []byte, name string) (interface{}, error) {
	mu.RLock()
	defer mu.RUnlock()

	c, ok := registry[name]
	if !ok {
		return nil, ErrUnregisteredMessageType
	}
	v := reflect.New(reflect.TypeOf(c)).Interface()
	err := protoUnmarshal(buf, v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func TypeName(v interface{}) string {
	rt := reflect.TypeOf(v)
	name := rt.String()
	if name[0] == '*' {
		return name[1:]
	}
	return name
}

func protoMarshal(v interface{}) ([]byte, error) {
	pb := v.(proto.Message)
	return proto.Marshal(pb)
}

func protoUnmarshal(buf []byte, v interface{}) error {
	pb := v.(proto.Message)
	return proto.Unmarshal(buf, pb)
}
