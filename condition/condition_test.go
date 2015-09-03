package condition

import (
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type TestingState struct {
	Count int `json:"count"`
}

func NewTestingState() *TestingState {
	return &TestingState{Count: 0}
}

func TestState(t *testing.T) {
	e := etcd.NewClient([]string{"http://localhost:2379"})

	s0 := NewState(e, 2*time.Second, "testing", "state")
	s1 := NewState(e, 2*time.Second, "testing", "state")
	va := NewTestingState()
	va.Count = 1

	var err error
	var stale bool

	// Create the initial value in Etcd.
	err = s0.Init(va)
	if err != nil {
		t.Fatalf("s0: failed init with error: %v", err)
	}

	// Read the value from Etcd, with the
	// expectation that there will be
	// no error due to a stale Etcd
	// index.
	vb := NewTestingState()
	stale, err = s0.Fetch(vb)
	if err != nil {
		t.Fatalf("s0: failed fetch with error: %v", err)
	}
	if stale {
		t.Fatal("s0: failed fetch with stale set 'true', but should have been 'false'")
	}

	// Data read should match previous data.
	if va.Count != vb.Count {
		t.Fatalf("s0: data saved in init did not batch data read with fetch, %v != %v", va.Count, vb.Count)
	}
	vb.Count = 2
	stale, err = s0.Store(vb)
	if err != nil {
		t.Fatalf("s0: failed store with error: %v", err)
	}
	if stale {
		t.Fatal("s0: failed store with stale set 'true', but should have been 'false'")
	}

	// Now set things up to perform
	// a stale store via s1.
	vc := NewTestingState()
	stale, err = s1.Store(vc)
	if err == nil {
		t.Fatal("s1: store should have failed with error, but did not")
	}
	if !stale {
		t.Fatal("s1: store should have failed with stale set 'true', but did not")
	}

	// Do another update to state via s0.
	vb.Count = 3
	stale, err = s0.Store(vb)
	if err != nil {
		t.Fatalf("s0: store failed with error: %v", err)
	}
	if stale {
		t.Fatal("s0: store failed with stale set 'true', but should have been 'falst'")
	}

	// Now set things up to perform
	// a stale read via s1.
	vd := NewTestingState()
	stale, err = s1.Fetch(vd)
	if err != nil {
		t.Fatalf("s1: failed fetch with error: %v", err)
	}
	if !stale {
		t.Fatalf("s1: failed fetch with stale set 'false', but should have been set 'true'")
	}
}
