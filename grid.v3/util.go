package grid

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
)

// NewSeededRand creates a new math/rand with a strong
// seed from cyrpto/rand.
func NewSeededRand() (*rand.Rand, error) {
	bytes := make([]byte, 8)
	n, err := cryptorand.Read(bytes)
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, fmt.Errorf("insufficient entropy")
	}
	seed, n := binary.Varint(bytes)
	if n != 8 {
		return nil, fmt.Errorf("failed to parse seed")
	}

	return rand.New(rand.NewSource(seed)), nil
}
