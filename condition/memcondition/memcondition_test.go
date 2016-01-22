package memcondition

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestMemoryCreateAndGet(t *testing.T) {
	mem := NewMemory()

	mem.CreateDir("/top1")
	mem.Create("/top1/C", "Z")
	mem.CreateDir("/top1/top2")
	mem.Create("/top1/top2/A", "X")
	mem.Create("/top1/top2/B", "Y")

	r, err := mem.Get("/top1")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, r.Node.Dir)
	assert.Equal(t, "/top1", r.Node.Key)
	assert.Equal(t, "", r.Node.Value)
	assert.Equal(t, uint64(1), r.Node.Index)

	r, err = mem.Get("/top1/C")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, r.Node.Dir)
	assert.Equal(t, "/top1/C", r.Node.Key)
	assert.Equal(t, "Z", r.Node.Value)
	assert.Equal(t, uint64(2), r.Node.Index)

	r, err = mem.Get("/top1/top2")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, r.Node.Dir)
	assert.Equal(t, "/top1/top2", r.Node.Key)
	assert.Equal(t, "", r.Node.Value)
	assert.Equal(t, uint64(3), r.Node.Index)

	r, err = mem.Get("/top1/top2/A")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, r.Node.Dir)
	assert.Equal(t, "/top1/top2/A", r.Node.Key)
	assert.Equal(t, "X", r.Node.Value)
	assert.Equal(t, uint64(4), r.Node.Index)

	r, err = mem.Get("/top1/top2/B")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, r.Node.Dir)
	assert.Equal(t, "/top1/top2/B", r.Node.Key)
	assert.Equal(t, "Y", r.Node.Value)
	assert.Equal(t, uint64(5), r.Node.Index)
}

func TestMemoryUpdate(t *testing.T) {
	mem := NewMemory()

	mem.CreateDir("/top1")
	mem.Create("/top1/A", "X1")

	r, err := mem.Get("/top1")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, r.Node.Dir)
	assert.Equal(t, "/top1", r.Node.Key)
	assert.Equal(t, "", r.Node.Value)
	assert.Equal(t, uint64(1), r.Node.Index)

	r, err = mem.Get("/top1/A")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, r.Node.Dir)
	assert.Equal(t, "/top1/A", r.Node.Key)
	assert.Equal(t, "X1", r.Node.Value)
	assert.Equal(t, uint64(2), r.Node.Index)

	r, err = mem.Update("/top1/A", "X2")
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(5), r.Index)

	r, err = mem.Get("/top1/A")
	assert.Equal(t, false, r.Node.Dir)
	assert.Equal(t, "/top1/A", r.Node.Key)
	assert.Equal(t, "X2", r.Node.Value)
	assert.Equal(t, uint64(5), r.Node.Index)
	assert.Equal(t, uint64(6), r.Index)
}

func TestMemoryDelete(t *testing.T) {
	mem := NewMemory()

	mem.CreateDir("/top1")
	mem.Create("/top1/A", "X1")

	r, err := mem.Get("/top1")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, r.Node.Dir)
	assert.Equal(t, "/top1", r.Node.Key)
	assert.Equal(t, "", r.Node.Value)
	assert.Equal(t, uint64(1), r.Node.Index)

	r, err = mem.Get("/top1/A")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, r.Node.Dir)
	assert.Equal(t, "/top1/A", r.Node.Key)
	assert.Equal(t, "X1", r.Node.Value)
	assert.Equal(t, uint64(2), r.Node.Index)

	r, err = mem.Delete("/top1/A")
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(5), r.Index)

	r, err = mem.Get("/top1/A")
	assert.Equal(t, ErrKeyDoesNotExist, err)
	assert.Equal(t, (*Response)(nil), r)

	r, err = mem.Delete("/top1")
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(7), r.Index)

	r, err = mem.Get("/top1")
	assert.Equal(t, ErrKeyDoesNotExist, err)
	assert.Equal(t, (*Response)(nil), r)
}

func TestMemoryErrors(t *testing.T) {
	mem := NewMemory()

	mem.CreateDir("/top1")
	mem.Create("/top1/A", "X")

	r, err := mem.Get("/top1/B")
	assert.Equal(t, ErrKeyDoesNotExist, err)
	assert.Equal(t, (*Response)(nil), r)

	r, err = mem.CreateDir("/top1")
	assert.Equal(t, ErrKeyExists, err)
	assert.Equal(t, (*Response)(nil), r)

	r, err = mem.Create("/top1/A", "X")
	assert.Equal(t, ErrKeyExists, err)
	assert.Equal(t, (*Response)(nil), r)
}

func TestMemoryDescribe(t *testing.T) {
	expected :=
		`/top1:1
 /top1/C:Z:2
 /top1/top2:3
  /top1/top2/A:X:4
  /top1/top2/B:Y:5
`
	mem := NewMemory()
	mem.CreateDir("/top1")
	mem.Create("/top1/C", "Z")
	mem.CreateDir("/top1/top2")
	mem.Create("/top1/top2/A", "X")
	mem.Create("/top1/top2/B", "Y")

	assert.Equal(t, expected, mem.Describe())
}
