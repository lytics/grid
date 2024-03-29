package grid

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestIsNameValidEmpty(t *testing.T) {
	t.Parallel()
	if isNameValid("") {
		t.Fatal("expected false")
	}
}

func TestIsNameValidBadChars(t *testing.T) {
	t.Parallel()
	for _, c := range strings.Split("( ) ` ~ ! @ # $ % ^ & * + = | \\ { } [ ] : ; ' < > , . ? /", "") {
		name := fmt.Sprintf("some-name-%s-that-is-bad", c)
		if isNameValid(name) {
			t.Fatalf("expected false with name containing: %s", c)
		}
	}
}

func TestStripNamespaceWithInvalidName(t *testing.T) {
	t.Parallel()
	_, err := stripNamespace(Peers, "ns", "hello")
	if !errors.Is(err, ErrInvalidName) {
		t.Fatal("expected invalid name error")
	}
}

func TestStripNamespace(t *testing.T) {
	t.Parallel()
	// Peers
	{
		res, err := stripNamespace(Peers, "ns", "ns.peer.hello")
		if err != nil {
			t.Fatal(err)
		}
		if res != "hello" {
			t.Fatal("expected name without namespace")
		}
	}

	// Actors
	{
		res, err := stripNamespace(Actors, "ns", "ns.actor.hello")
		if err != nil {
			t.Fatal(err)
		}
		if res != "hello" {
			t.Fatal("expected name without namespace")
		}
	}

	// Mailboxes
	{
		res, err := stripNamespace(Mailboxes, "ns", "ns.mailbox.hello")
		if err != nil {
			t.Fatal(err)
		}
		if res != "hello" {
			t.Fatal("expected name without namespace")
		}
	}
}

func TestNamespaceNameInvalidName(t *testing.T) {
	t.Parallel()
	_, err := namespaceName(Peers, "valid", "invalid-!")
	if !errors.Is(err, ErrInvalidName) {
		t.Fatal("expected invalid name error")
	}
}

func TestNamespaceNameInvalidNamespace(t *testing.T) {
	t.Parallel()
	_, err := namespaceName(Peers, "invalid-!", "valid")
	if !errors.Is(err, ErrInvalidNamespace) {
		t.Fatal("expected invalid namespace error")
	}
}

func TestNamespacePrefixInvalidNamespace(t *testing.T) {
	t.Parallel()
	_, err := namespacePrefix(Peers, "invalid-!")
	if !errors.Is(err, ErrInvalidNamespace) {
		t.Fatal("expected invalid namespace error")
	}
}
