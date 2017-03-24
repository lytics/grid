package ring

import (
	"fmt"
	"hash/fnv"
	"math/rand"
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

func NewMultiRing(name string, members, totalRings, reservedRings int) MultiRing {
	rings := make([]Ring, totalRings)
	for i := 0; i < totalRings; i++ {
		ringName := fmt.Sprintf("%v-%v", name, i)
		r := New(ringName, members)
		r.(*ring).actortype = name
		rings[i] = r
	}
	return &multi{
		totalRings:    totalRings,
		reservedRings: reservedRings,
		dice:          rand.New(rand.NewSource(rand.Int63())),
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
