package grid

import (
	"fmt"
	"regexp"
	"sync"
)

const validActorName = "^[a-zA-Z0-9-_]+$"

var (
	validActorNameRegEx *regexp.Regexp = regexp.MustCompile(validActorName)
	knownNamespaces     sync.Map
)

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
		return "", fmt.Errorf("%w: namespace=%s fullname=%s plen=%d", ErrInvalidName, namespace, fullname, plen)
	}
	return fullname[plen:], nil
}

func namespaceName(t EntityType, namespace, name string) (string, error) {
	if !isNameValid(name) {
		return "", fmt.Errorf("%w: namespace=%s name=%s", ErrInvalidName, namespace, name)
	}
	// an optimization to reduce memory allocations is to
	// check if we've seen this namespace before.  To avoid allocating
	// a regEx on each request.
	if _, seen := knownNamespaces.Load(namespace); !seen {
		if !isNameValid(namespace) {
			return "", fmt.Errorf("%w: namespace=%s name=%s", ErrInvalidNamespace, namespace, name)
		}
		knownNamespaces.Store(namespace, struct{}{})
	}

	return fmt.Sprintf("%v.%v.%v", namespace, t, name), nil
}

func namespacePrefix(t EntityType, namespace string) (string, error) {
	if _, seen := knownNamespaces.Load(namespace); !seen {
		if !isNameValid(namespace) {
			return "", fmt.Errorf("%w: namespace=%s", ErrInvalidNamespace, namespace)
		}
		knownNamespaces.Store(namespace, struct{}{})
	}
	return fmt.Sprintf("%v.%v.", namespace, t), nil
}
