package grid

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMailboxRegistry(t *testing.T) {
	t.Parallel()
	m := newMailboxRegistry()
	require.NotZero(t, m)
	assert.NotZero(t, m.r)
}

func TestMailboxRegistryGetSetDeleteSize(t *testing.T) {
	t.Parallel()

	const n1 = "n1"
	const n2 = "n2"

	m1 := &Mailbox{name: "m1"}
	m2 := &Mailbox{name: "m2"}
	m3 := &Mailbox{name: "m3"}

	r := newMailboxRegistry()

	// Set one
	_, ok := r.Get(n1)
	assert.False(t, ok)
	assert.Equal(t, 0, r.Size())

	update := r.Set(n1, m1)
	assert.False(t, update)
	assert.Equal(t, 1, r.Size())

	got1, ok := r.Get(n1)
	assert.True(t, ok)
	assert.Equal(t, m1, got1)

	// Set second
	update = r.Set(n2, m2)
	assert.False(t, update)
	assert.Equal(t, 2, r.Size())

	got1, ok = r.Get(n1)
	assert.True(t, ok)
	assert.Equal(t, m1, got1)

	got2, ok := r.Get(n2)
	assert.True(t, ok)
	assert.Equal(t, m2, got2)

	// Overwrite first
	update = r.Set(n1, m3)
	assert.True(t, update)
	assert.Equal(t, 2, r.Size())

	got1, ok = r.Get(n1)
	assert.True(t, ok)
	assert.Equal(t, m3, got1)

	got2, ok = r.Get(n2)
	assert.True(t, ok)
	assert.Equal(t, m2, got2)

	// Delete non-existent
	ok = r.Delete("some-name")
	assert.False(t, ok)
	assert.Equal(t, 2, r.Size())

	// Delete first
	ok = r.Delete(n1)
	assert.True(t, ok)
	assert.Equal(t, 1, r.Size())

	_, ok = r.Get(n1)
	assert.False(t, ok)

	got2, ok = r.Get(n2)
	assert.True(t, ok)
	assert.Equal(t, m2, got2)

	// Delete second
	ok = r.Delete(n2)
	assert.True(t, ok)
	assert.Equal(t, 0, r.Size())

	_, ok = r.Get(n2)
	assert.False(t, ok)
}

func TestMailboxRegistryR(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 2} {
		n := n
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()

			r := newMailboxRegistry()
			ms := r.R()
			assert.NotNil(t, ms)
			assert.Empty(t, ms)

			msl := make([]*Mailbox, n)
			for i := 0; i < n; i++ {
				name := strconv.Itoa(i)
				m := Mailbox{name: name}
				msl[i] = &m
				r.Set(name, &m)
			}

			ms = r.R()
			require.Len(t, ms, n)
			for _, m := range msl {
				got, ok := ms[m.name]
				assert.True(t, ok)
				assert.Equal(t, m, got)
			}
		})
	}
}
