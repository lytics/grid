package codec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	// FIXME (2022-05) (mh): Recompile proto and use google.golang.org/protobuf
	"google.golang.org/protobuf/proto"
)

var (
	// ErrUnsupportedMessage when register is called with a type
	// which cannot be en/decoded by the codec.
	ErrUnsupportedMessage = errors.New("codec: unsupported message")
	// ErrUnregisteredMessageType when a unregistered type is called
	// for marshalling or unmarshalling.
	ErrUnregisteredMessageType = errors.New("codec: unregistered message type")
)

var (
	mu       = &sync.RWMutex{}
	registry = map[string]any{}
)

// Register a type for marshalling and unmarshalling.
// The type must currently implement proto.Message.
func Register(v any) error {
	mu.Lock()
	defer mu.Unlock()

	// The value 'v' must not be registered
	// as a pointer type, but to check if
	// it is a proto message, the pointer
	// type must be checked.
	pv := reflect.New(reflect.TypeOf(v)).Interface()

	_, ok := pv.(proto.Message)
	if !ok {
		return ErrUnsupportedMessage
	}

	name := TypeName(v)
	registry[name] = v
	// TODO(aj) Temporary migration solution for go module upgrade
	if after, ok0 := strings.CutPrefix(name, "github.com/lytics/lio/vendor/"); ok0 {
		// If we're the pre go.mod, register the other namespace
		otherName := after
		registry[otherName] = v
	} else {
		// If we're using go.mod, register the old namespace
		otherName := fmt.Sprintf("github.com/lytics/lio/vendor/%s", name)
		registry[otherName] = v
	}
	return nil
}

// Marshal the value into bytes. The function returns
// the type name, the bytes, or an error.
func Marshal(v any) (string, []byte, error) {
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

// Unmarshal the bytes into a value whos type is given,
// or return an error.
func Unmarshal(buf []byte, name string) (any, error) {
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

// TypeName of a value. This name is used in the registry
// to distinguish types.
func TypeName(v any) string {
	rt := reflect.TypeOf(v)
	pkg := rt.PkgPath()
	name := rt.Name()
	if name == "" {
		rt = rt.Elem()
		pkg = rt.PkgPath()
		name = rt.Name()
	}
	return pkg + "/" + name
}

func protoMarshal(v any) ([]byte, error) {
	pb := v.(proto.Message)
	return proto.Marshal(pb)
}

func protoUnmarshal(buf []byte, v any) error {
	pb := v.(proto.Message)
	return proto.Unmarshal(buf, pb)
}
