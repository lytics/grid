package ring

import (
	"fmt"
	"hash/fnv"

	"github.com/lytics/grid"
)

type Ring interface {
	Names() []string
	RouteByModulo(n int, m interface{})
}

type ring struct {
	g      grid.Grid
	name   string
	nparts int
}

func New(name string, parts int, g grid.Grid) Ring {
	return &ring{name: name, parts: parts, g: g}
}

// Names returns the list of actor names in this ring. They
// may or may not be running.
func (r *ring) Names() []string {
	names := make([]string, r.nparts)
	for i := 0; i < r.nparts; i++ {
		names[i] = r.actorName(i)
	}
	return names
}

// ByModuloInt selects an actor name by using:
//     key % number of actors
func (r *ring) ByInt(key int) string {
	part := key % r.nparts
	return r.actorName(part)
}

// ByModuloUint32 selects an actor name by using:
//     key % number of actors
func (r *ring) ByUint32(key uint32) string {
	part := key % uint32(r.nparts)
	return r.actorName(int(part))
}

// ByModuloUint64 selects an actor name by using:
//     key % number of actors
func (r *ring) ByUint64(key uint64) string {
	part := key % uint64(r.nparts)
	return r.actorName(int(part))
}

// ByHashBytes selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedBytes(key []byte) string {
	h := fnv.New64()
	h.Write(key)
	part := h.Sum64() % uint64(r.nparts)
	return r.actorName(int(part))
}

// ByHashedString selects an actor name by using:
//     hash(key) % number of actors
func (r *ring) ByHashedString(key string) string {
	h := fnv.New64()
	h.Write([]byte(key))
	part := h.Sum64() % uint64(r.nparts)
	return r.actorName(int(part))
}

func (r *ring) actorName(part int) string {
	fmt.Sprintf("%s.%s.ring.%d.%d", r.g.Name, r.name, r.nparts, part)
}
