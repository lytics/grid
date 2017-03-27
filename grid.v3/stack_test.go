package grid

// This test need to be fixed up so it can work on computers other than mdmarkek
// I left it trying to replace numbers and the GOROOT.
//
// func TestNiceStack(t *testing.T) {
// 	expected := `/usr/local/go/src/runtime/debug/stack.go:24 <-- /home/mdmarek/src/github.com/lytics/grid/grid.v3/stack_test.go:15 <-- /usr/local/go/src/runtime/panic.go:458 <-- /home/mdmarek/src/github.com/lytics/grid/grid.v3/stack_test.go:18 <-- /home/mdmarek/src/github.com/lytics/grid/grid.v3/stack_test.go:21 <-- /usr/local/go/src/testing/testing.go:610 <-- /usr/local/go/src/testing/testing.go:646`

// 	reDir := regexp.MustCompile(runtime.GOROOT())
// 	re := regexp.MustCompile("[0-9]")
// 	expected = re.ReplaceAllString(expected, "")

// 	var actual string
// 	f := func() {
// 		defer func() {
// 			if err := recover(); err != nil {
// 				actual = niceStack(debug.Stack())
// 			}
// 		}()
// 		panic("show stack trace")
// 	}
// 	actual = re.ReplaceAllString(actual, "")
// 	actual = reDir.ReplaceAllString(actual, "")

// 	f()
// 	if expected != actual {
// 		t.Logf("expected: %v", expected)
// 		t.Logf("  actual: %v", actual)
// 		t.Fail()
// 	}
// }
