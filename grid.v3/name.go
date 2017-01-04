package grid

import "fmt"

func stripNamespace(namespace, fullname string) (string, error) {
	plen := len(namespace) + 1
	if len(fullname) <= plen {
		return "", ErrInvalidName
	}
	return fullname[plen:], nil
}

func namespaceName(namespace, name string) (string, error) {
	if !isNameValid(name) {
		return "", ErrInvalidName
	}
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v", namespace, name), nil
}
