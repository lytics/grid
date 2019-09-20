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
	expected := `stack.go <-- stack_test.go <-- panic.go <-- stack_test.go <-- stack_test.go <-- testing.go <-- testing.go`

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

	// Rework the actual result into a string that
	// works across systems since local paths are
	// placed in the stack trace.
	actual := ""
	for i, part := range strings.Split(recovered, " <-- ") {
		//f := strings.Index(part, pkg) // First
		f := strings.LastIndex(part, "/") // First
		l := strings.Index(part, ":")     // Last
		if f < 0 {
			f = 0
		}
		if i == 0 {
			actual = part[f+1 : l]
		} else {
			actual = actual + " <-- " + part[f+1:l]
		}
	}
	if expected != actual {
		t.Logf("expected: %v", expected)
		t.Logf("  actual: %v", actual)
		t.Fail()
	}
}
