package grid

import (
	"errors"
	"regexp"

	"golang.org/x/net/context"
)

var (
	ErrInvalidActorName      = errors.New("invalid actor name")
	ErrInvalidActorNamespace = errors.New("invalid actor namespace")
)

type Actor interface {
	Act(c context.Context)
}

const validActorName = "^[a-zA-Z0-9-_]+$"

// IsNameValid returns true if the give name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func isNameValid(name string) bool {
	if matched, err := regexp.MatchString(validActorName, name); err != nil {
		return false
	} else {
		return matched
	}
}
