package codec

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrUnregisteredMessageType = errors.New("grid: unregistered message type")

type codecType struct {
	proto interface{} // The "prototype".
	codec Codec       // The codec to encode/decode such prototypes.
}

var reg = map[string]*codecType{}

func Register(v interface{}, c Codec) {
	name := TypeName(v)
	fmt.Printf("registering: %v\n", name)
	reg[name] = &codecType{
		proto: v,
		codec: c,
	}
}

func Marshal(v interface{}) ([]byte, error) {
	name := TypeName(v)
	fmt.Printf("marshal: %T, %v\n", v, name)
	c, ok := reg[name]
	if !ok {
		return nil, ErrUnregisteredMessageType
	}
	return c.codec.Marshal(v)
}

func Unmarshal(buf []byte, name string) (interface{}, error) {
	fmt.Printf("registry unmarshal: %v\n", name)
	c, ok := reg[name]
	if !ok {
		return nil, ErrUnregisteredMessageType
	}
	v := reflect.New(reflect.TypeOf(c.proto)).Interface()
	err := c.codec.Unmarshal(buf, v)
	if err != nil {
		fmt.Printf(">>> codec unmarshal: %T, %v, %v\n", v, name, err)
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
