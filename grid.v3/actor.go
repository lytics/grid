package grid

import (
	"context"
	"regexp"
)

// Actor that does work.
type Actor interface {
	Act(c context.Context)
}

// isNameValid returns true if the give name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func isNameValid(name string) bool {
	const validActorName = "^[a-zA-Z0-9-_]+$"

	if name == "" {
		return false
	}
	if matched, err := regexp.MatchString(validActorName, name); err != nil {
		return false
	} else {
		return matched
	}
}
