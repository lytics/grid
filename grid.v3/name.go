package grid

import "fmt"

type entityType string

const (
	Peers     entityType = "peer"
	Actors    entityType = "actor"
	Mailboxes entityType = "mailbox"
)

func stripNamespace(t entityType, namespace, fullname string) (string, error) {
	plen := len(namespace) + 1 + len(t) + 1
	if len(fullname) <= plen {
		return "", ErrInvalidName
	}
	return fullname[plen:], nil
}

func namespaceName(t entityType, namespace, name string) (string, error) {
	if !isNameValid(name) {
		return "", ErrInvalidName
	}
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.%v", namespace, t, name), nil
}

func namespacePrefix(t entityType, namespace string) (string, error) {
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.", namespace, t), nil
}
