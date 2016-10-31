package grid

import (
	"context"
	"regexp"
)

type Actor interface {
	Act(c context.Context)
}

const validActorName = "^[a-zA-Z0-9-_]+$"

// isNameValid returns true if the give name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func isNameValid(name string) bool {
	if name == "" {
		return false
	}
	if matched, err := regexp.MatchString(validActorName, name); err != nil {
		return false
	} else {
		return matched
	}
}
