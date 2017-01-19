package grid

import "fmt"

type nameClass string

const (
	peerClass    nameClass = "peer"
	actorClass   nameClass = "actor"
	mailboxClass nameClass = "mailbox"
)

func stripNamespace(c nameClass, namespace, fullname string) (string, error) {
	plen := len(namespace) + 1 + len(c) + 1
	if len(fullname) <= plen {
		return "", ErrInvalidName
	}
	return fullname[plen:], nil
}

func namespaceName(c nameClass, namespace, name string) (string, error) {
	if !isNameValid(name) {
		return "", ErrInvalidName
	}
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.%v", namespace, c, name), nil
}

func namespacePrefix(c nameClass, namespace string) (string, error) {
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.", namespace, c), nil
}
