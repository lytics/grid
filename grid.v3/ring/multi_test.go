package ring

import "testing"

func TestMultiRing(t *testing.T) {
	const (
		name      = "reader"
		namespace = "testing"
	)

	expected := make(map[string]bool)
	expected["testing-reader-0-0"] = true
	expected["testing-reader-0-1"] = true
	expected["testing-reader-1-0"] = true
	expected["testing-reader-1-1"] = true

	m := NewMultiRing(namespace, name, 2, 2, 0)
	for _, r := range m.Rings() {
		for _, def := range r.ActorDefs() {
			if !expected[def.ID()] {
				t.Fatalf("expected to find: %v", def.ID())
			}
			if def.Type != "reader" {
				t.Fatalf("expected actor type to be 'reader'")
			}
			delete(expected, def.ID())
		}
	}
	if len(expected) != 0 {
		t.Fatalf("multi-ring did not create all expected actors")
	}
}

func TestMultiRingByHashedString(t *testing.T) {
	const (
		name      = "reader"
		namespace = "namespace"
	)

	m := NewMultiRing(namespace, name, 2, 20, 0)

	if r := m.ByHashedString("group-0"); r.(*ring).name != "reader-17" {
		t.Fatalf("expected '%v' to hash to ring '%v'", "group-0", "")
	}

	if r := m.ByHashedString("group-1"); r.(*ring).name != "reader-16" {
		t.Fatalf("expected '%v' to hash to ring '%v'", "group-1", "reader-16")
	}

	if r := m.ByHashedString("group-2"); r.(*ring).name != "reader-19" {
		t.Fatalf("expected '%v' to hash to ring '%v'", "group-2", "reader-19")
	}

	if r := m.ByHashedString("group-3"); r.(*ring).name != "reader-18" {
		t.Fatalf("expected '%v' to hash to ring '%v'", "group-3", "reader-18")
	}

	if r := m.ByHashedString("group-4"); r.(*ring).name != "reader-1" {
		t.Fatalf("expected '%v' to hash to ring '%v'", "group-4", "reader-1")
	}
}
