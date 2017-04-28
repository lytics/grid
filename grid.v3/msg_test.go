package grid

import (
	"fmt"
	"testing"

	"github.com/lytics/grid/grid.v3/codec"
)

func TestMarshalUnmarshalActorStart(t *testing.T) {
	msg0 := NewActorStart("testing")
	data, err := codec.Marshal(msg0)
	if err != nil {
		t.Fatal(err)
	}

	name := codec.TypeName(msg0)

	msg1, err := codec.Unmarshal(data, name)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(msg1)
}
