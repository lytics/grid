package grid

import (
	"runtime/debug"
	"strings"
	"testing"
)

// TestNiceStack tests that the niceStack function transforms a multiline
// stack trace into a single line stack trace and into a "clean" format.
// This is done so that stack traces can be logged on a single line for
// systems that don't parse multi-line log messages, and to make the
// message easy to read.
func TestNiceStack(t *testing.T) {
	// Expected is actually "fixed up" to remove line
	// numbers and local paths.
	expected := `/usr/local/go/src/runtime/debug/stack.go <-- github.com/lytics/grid/grid.v3/stack_test.go <-- /usr/local/go/src/runtime/panic.go <-- github.com/lytics/grid/grid.v3/stack_test.go <-- github.com/lytics/grid/grid.v3/stack_test.go <-- /usr/local/go/src/testing/testing.go <-- /usr/local/go/src/testing/testing.go`

	var recovered string
	f := func() {
		defer func() {
			if err := recover(); err != nil {
				recovered = niceStack(debug.Stack())
			}
		}()
		panic("show stack trace")
	}
	f()

	// Strings that are used as "tokens" for checking
	// parts of strings to be ignored. Things before
	// the package, and after the number need to be
	// ignored since they change from system to system
	// and with none code changes like additional new
	// lines.
	const (
		pkg = "github.com/lytics/grid/grid.v3"
		num = ":"
	)

	// Rework the actual result into a string that
	// works across systems since local paths are
	// placed in the stack trace.
	actual := ""
	for i, part := range strings.Split(recovered, " <-- ") {
		f := strings.Index(part, pkg) // First
		l := strings.Index(part, ":") // Last
		if f < 0 {
			f = 0
		}
		if i == 0 {
			actual = part[f:l]
		} else {
			actual = actual + " <-- " + part[f:l]
		}
	}
	if expected != actual {
		t.Logf("expected: %v", expected)
		t.Logf("  actual: %v", actual)
		t.Fail()
	}
}
