package grid

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand"
)

// NewSeededRand creates a new math/rand with a strong
// seed from cyrpto/rand.
func NewSeededRand() *rand.Rand {
	bytes := make([]byte, 8)
	n, err := cryptorand.Read(bytes)
	if err != nil {
		panic(err.Error())
	}
	if n != 8 {
		panic("insufficient entropy")
	}
	seed, n := binary.Varint(bytes)
	if n == 0 {
		panic("failed to parse seed")
	}
	return rand.New(rand.NewSource(seed))
}
