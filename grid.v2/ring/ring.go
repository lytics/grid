package ring

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/lytics/grid"
)

// Ring represents a set of actor members used to divide a data
// space into disjoint parts, each part owned by a particular
// actor in the ring.
type Ring interface {
	ID() string
	ActorDefs() []*grid.ActorDef
	ByInt(key int) string
	ByUint32(key uint32) string
	ByUint64(key uint64) string
	ByHashedBytes(key []byte) string
	ByHashedString(key string) string
	ByRandom() string
}

type ring struct {
	dice      *rand.Rand
	name      string
	actortype string
	n         int
}

func New(name string, n int) Ring {
	return &ring{dice: grid.NewSeededRand(), name: name, actortype: name, n: n}
}

func (r *ring) ID() string {
	return r.name
}

// ActorDefs returns the list of actor names in this ring. They
// may or may not be running.
func (r *ring) ActorDefs() []*grid.ActorDef {
	names := make([]*grid.ActorDef, r.n)
	for i := 0; i < r.n; i++ {
		names[i] = r.actorDef(i)
	}
	return names
}

// ByRandom selects an actor name by random.
func (r *ring) ByRandom() string {
	return r.actorName(r.dice.Intn(r.n))
}

// ByModuloInt selects an actor name by using:
//     key % number of actors
func (r *ring) ByInt(key int) string {
	i := key % r.n
	return r.actorName(i)
}

// ByModuloUint32 selects an actor name by using:
//     key % number of actors
func (r *ring) ByUint32(key uint32) string {
	i := key % uint32(r.n)
	return r.actorName(int(i))
}

// ByModuloUint64 selects an actor name by using:
//     key % number of actors
func (r *ring) ByUint64(key uint64) string {
	i := key % uint64(r.n)
	return r.actorName(int(i))
}

// ByHashBytes selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedBytes(key []byte) string {
	h := fnv.New64()
	h.Write(key)
	i := h.Sum64() % uint64(r.n)
	return r.actorName(int(i))
}

// ByHashedString selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedString(key string) string {
	h := fnv.New64()
	h.Write([]byte(key))
	i := h.Sum64() % uint64(r.n)
	return r.actorName(int(i))
}

// ByHashedInt selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedInt(key int) string {
	b := make([]byte, binary.MaxVarintLen64)
	len := binary.PutVarint(b, int64(key))
	if len == 0 {
		panic(fmt.Sprintf("failed to binary encode key: %v", key))
	}
	h := fnv.New64()
	h.Write(b)
	i := h.Sum64() % uint64(r.n)
	return r.actorName(int(i))
}

// ByHashedUint32 selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedUint32(key uint32) string {
	b := make([]byte, binary.MaxVarintLen64)
	len := binary.PutUvarint(b, uint64(key))
	if len == 0 {
		panic(fmt.Sprintf("failed to binary encode key: %v", key))
	}
	h := fnv.New64()
	h.Write(b)
	i := h.Sum64() % uint64(r.n)
	return r.actorName(int(i))
}

// ByHashedUint64 selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedUint64(key uint64) string {
	b := make([]byte, binary.MaxVarintLen64)
	len := binary.PutUvarint(b, key)
	if len == 0 {
		panic(fmt.Sprintf("failed to binary encode key: %v", key))
	}
	h := fnv.New64()
	h.Write(b)
	i := h.Sum64() % uint64(r.n)
	return r.actorName(int(i))
}

func (r *ring) actorDef(i int) *grid.ActorDef {
	a := grid.NewActorDef(r.actorName(i))
	a.DefineType(r.actortype)
	return a
}

func (r *ring) actorName(i int) string {
	return fmt.Sprintf("%s-%d", r.name, i)
}
