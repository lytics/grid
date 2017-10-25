package grid

import (
	"fmt"
	"strings"
	"testing"
)

func TestIsNameValidEmpty(t *testing.T) {
	if isNameValid("") {
		t.Fatal("expected false")
	}
}

func TestIsNameValidBadChars(t *testing.T) {
	for _, c := range strings.Split("( ) ` ~ ! @ # $ % ^ & * - + = | \\ { } [ ] : ; ' < > , . ? /", "") {
		name := fmt.Sprintf("some-name-@s-that-is-bad")
		if isNameValid(name) {
			t.Fatalf("expected false with name containing: %s", c)
		}
	}
}

func TestStripNamespaceWithInvalidName(t *testing.T) {
	_, err := stripNamespace(Peers, "ns", "hello")
	if err != ErrInvalidName {
		t.Fatal("expected invalid name error")
	}
}

func TestStripNamespace(t *testing.T) {
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
	_, err := namespaceName(Peers, "valid", "invalid-!")
	if err != ErrInvalidName {
		t.Fatal("expected invalid name error")
	}
}

func TestNamespaceNameInvalidNamespace(t *testing.T) {
	_, err := namespaceName(Peers, "invalid-!", "valid")
	if err != ErrInvalidNamespace {
		t.Fatal("expected invalid namespace error")
	}
}

func TestNamespacePrefixInvalidNamespace(t *testing.T) {
	_, err := namespacePrefix(Peers, "invalid-!")
	if err != ErrInvalidNamespace {
		t.Fatal("expected invalid namespace error")
	}
}
