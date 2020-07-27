package grid

import (
	"fmt"
	"regexp"
)

const validActorName = "^[a-zA-Z0-9-_]+$"

var validActorNameRegEx *regexp.Regexp = regexp.MustCompile(validActorName)

// isNameValid returns true if the name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func isNameValid(name string) bool {
	if name == "" {
		return false
	}
	return validActorNameRegEx.MatchString(name)
}

func stripNamespace(t EntityType, namespace, fullname string) (string, error) {
	plen := len(namespace) + 1 + len(t) + 1
	if len(fullname) <= plen {
		return "", ErrInvalidName
	}
	return fullname[plen:], nil
}

func namespaceName(t EntityType, namespace, name string) (string, error) {
	if !isNameValid(name) {
		return "", ErrInvalidName
	}
	// an optimization to reduce memory allocations is to
	// check if we've seen this namespace before.  To avoid allocating
	// a regEx on each request.
	_, seen := knownNamespaces[namespace]
	if !seen {
		if !isNameValid(namespace) {
			return "", ErrInvalidNamespace
		}
		knownNamespaces[namespace] = struct{}{}
	}
	return fmt.Sprintf("%v.%v.%v", namespace, t, name), nil
}

func namespacePrefix(t EntityType, namespace string) (string, error) {
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.", namespace, t), nil
}

var knownNamespaces = map[string]struct{}{}
