package ring

import (
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/lytics/grid/grid.v3"
)

type MultiRing interface {
	Rings() []Ring
	ByHashedString(key string) Ring
	ByReservedHashedString(key string) Ring
}

type multi struct {
	totalRings    int
	reservedRings int
	dice          *rand.Rand
	rings         []Ring
}

func NewMultiRing(namespace, name string, members, totalRings, reservedRings int) MultiRing {
	rings := make([]Ring, totalRings)
	for i := 0; i < totalRings; i++ {
		r := New(namespace, fmt.Sprintf("%v-%v", name, i), members)
		r.(*ring).actortype = name
		rings[i] = r
	}
	return &multi{
		totalRings:    totalRings,
		reservedRings: reservedRings,
		dice:          grid.NewSeededRand(),
		rings:         rings,
	}
}

func (m *multi) Rings() []Ring {
	rings := make([]Ring, m.totalRings)
	copy(rings, m.rings)
	return rings
}

func (m *multi) ByHashedString(key string) Ring {
	h := fnv.New64()
	h.Write([]byte(key))
	i := h.Sum64() % uint64(m.totalRings-m.reservedRings)
	return m.rings[i]
}

func (m *multi) ByReservedHashedString(key string) Ring {
	h := fnv.New64()
	h.Write([]byte(key))
	i := (m.totalRings - m.reservedRings) + int(h.Sum64()%uint64(m.reservedRings))
	return m.rings[i]
}
