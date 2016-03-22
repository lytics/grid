package ring

import (
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/lytics/grid"
)

type MultiRing interface {
	Rings() []Ring
	ByHashedString(key string) Ring
}

type multi struct {
	r     int
	dice  *rand.Rand
	rings []Ring
}

func NewMultiRing(name string, n, r int) MultiRing {
	rings := make([]Ring, r)
	for i := 0; i < r; i++ {
		r := New(fmt.Sprintf("%v-%v", name, i), n)
		r.(*ring).actortype = name
		rings[i] = r
	}
	return &multi{r: r, dice: grid.NewSeededRand(), rings: rings}
}

func (m *multi) Rings() []Ring {
	rings := make([]Ring, m.r)
	copy(rings, m.rings)
	return rings
}

func (m *multi) ByHashedString(key string) Ring {
	h := fnv.New64()
	h.Write([]byte(key))
	i := h.Sum64() % uint64(m.r)
	return m.rings[i]
}
