package grid

import (
	"errors"
	"strings"
	"testing"
)

func TestRespondWithAlreadyResponded(t *testing.T) {
	req := &request{finished: true}
	err := req.Respond("some-msg")
	if err != ErrAlreadyResponded {
		t.Fatal("expected error")
	}
}

func TestResponedWithError(t *testing.T) {
	expected := errors.New("expected-error")

	req := &request{failure: make(chan error, 1)}
	req.Respond(expected)

	select {
	case err := <-req.failure:
		if err != expected {
			t.Fatal(err)
		}
	default:
		t.Fatal("expected error")
	}
}

type badGobType struct{}

func TestGobEncodeError(t *testing.T) {
	req := &request{}
	err := req.Respond(&badGobType{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "type not registered for interface") {
		t.Fatal("expected specific error")
	}
}
