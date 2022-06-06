package grid

import (
	"fmt"
	"testing"
)

func newNamespace(t testing.TB) string {
	return fmt.Sprintf("test-namespace-%v", t.Name())
}
