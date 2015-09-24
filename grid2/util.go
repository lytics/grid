package grid2

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"time"
)

// NewSeededRand creates a *math/rand.Rand with a seed generated
// by NewSeed.
func NewSeededRand() *rand.Rand {
	return rand.New(rand.NewSource(NewSeed()))
}

// NewSeed generate a seed by running time.Now().UnixNano()
// through 1,000 rounds of an FNV hash.
func NewSeed() int64 {
	h := fnv.New64()
	for i := 0; i < 1000; i++ {
		b := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(b, time.Now().UnixNano())
		h.Write(b)
	}
	return int64(h.Sum64())
}
