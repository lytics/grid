package grid

import (
	"fmt"
	"math/rand"
)

func newNamespace() string {
	return fmt.Sprintf("test-namespace-%d", rand.Int63())
}
