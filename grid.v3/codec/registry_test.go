package codec

import (
	"reflect"
	"testing"

	"github.com/lytics/grid/grid.v3/codec/protomessage"
)

func TestTypeName(t *testing.T) {
	const (
		expected = "protomessage.Person"
	)

	msg := protomessage.Person{}
	rt := reflect.TypeOf(msg)

	if rt.String() != expected {
		t.Fatal("expected:", expected, " got:", rt.String())
	}
}

func TestRegisterMarshalUnmarshal(t *testing.T) {
	err := Register(protomessage.Person{})
	if err != nil {
		t.Fatal(err)
	}

	msg := &protomessage.Person{
		Name: "James Tester",
		Phones: []*protomessage.Person_PhoneNumber{
			&protomessage.Person_PhoneNumber{
				Number:    "555-555-5555",
				PhoneType: protomessage.Person_HOME,
			},
		},
	}
	typeName, data, err := Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	res, err := Unmarshal(data, typeName)
	switch res := res.(type) {
	case *protomessage.Person:
		if msg.Name != res.Name {
			t.Fatal("expected same name")
		}
		if msg.Phones[0].Number != res.Phones[0].Number {
			t.Fatal("expected same phone number")
		}
		if msg.Phones[0].PhoneType != res.Phones[0].PhoneType {
			t.Fatal("expected same phone type")
		}
	}
}

func TestNonProtobuf(t *testing.T) {
	notProto := "notProto"

	err := Register(notProto)
	if err != ErrNonProtoMessage {
		t.Fatal("expected error")
	}
}

// BenchmarkMarshal checks how fast it is to look up
// a type in the registry and marshal.
//
// Local results:
//     BenchmarkMarshal-4     3000000       400 ns/op
//
func BenchmarkMarshal(b *testing.B) {
	err := Register(protomessage.Person{})
	if err != nil {
		b.Fatal(err)
	}

	msg := &protomessage.Person{
		Name: "James Tester",
	}

	for i := 0; i < b.N; i++ {
		_, data, err := Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}
		if len(data) == 0 {
			b.Fatal("marshal produced zero bytes")
		}
	}
}

// BenchmarkUnmarshal checks how fast it is to look up
// a type in the registry and unmarshal.
//
// Local results:
//     BenchmarkUnmarshal-4   3000000       463 ns/op
//
func BenchmarkUnmarshal(b *testing.B) {
	err := Register(protomessage.Person{})
	if err != nil {
		b.Fatal(err)
	}

	msg := &protomessage.Person{
		Name: "James Tester",
	}
	typeName, data, err := Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		res, err := Unmarshal(data, typeName)
		if err != nil {
			b.Fatal(err)
		}
		if res.(*protomessage.Person).Name != "James Tester" {
			b.Fatal("wrong name")
		}
	}
}
