package codec

import (
	"reflect"
	"testing"

	"fmt"

	"github.com/lytics/grid/grid.v3/codec/gobmessage"
	"github.com/lytics/grid/grid.v3/codec/protomessage"
)

func TestNaming(t *testing.T) {
	msg0 := protomessage.Person{}
	msg1 := &protomessage.Person{}

	rt0 := reflect.TypeOf(msg0)
	rt1 := reflect.TypeOf(msg1)

	fmt.Println(rt0.String())
	fmt.Println(rt1.String())

}

func TestProtobufRegisterAndGet(t *testing.T) {
	Register(protomessage.Person{}, Protobuf)

	msg := &protomessage.Person{
		Name: "James Tester",
		Phones: []*protomessage.Person_PhoneNumber{
			&protomessage.Person_PhoneNumber{
				Number:    "555-555-5555",
				PhoneType: protomessage.Person_HOME,
			},
		},
	}
	buf, err := Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	msg1, err := Unmarshal(buf, TypeName(msg))
	fmt.Println(msg1)
}

func TestGobRegisterAndGet(t *testing.T) {
	Register(gobmessage.Person{}, Gob)

	msg := &gobmessage.Person{
		Name: "James Tester",
		Phones: []*gobmessage.PhoneNumber{
			&gobmessage.PhoneNumber{
				Number:    "555-555-5555",
				PhoneType: gobmessage.Home,
			},
		},
	}
	buf, err := Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	msg1, err := Unmarshal(buf, TypeName(msg))
	fmt.Println(msg1)
}
