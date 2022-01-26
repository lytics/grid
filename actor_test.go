package grid

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMakeRegistry(t *testing.T) {
	t.Parallel()
	m := newMakeActorRegistry()
	require.NotZero(t, m)
	assert.NotZero(t, m.r)
}

func TestMakeRegistryGetSet(t *testing.T) {
	t.Parallel()

	const n1 = "n1"
	const n2 = "n2"

	// we're using the errors here to distinguish between them
	e1 := errors.New("e1")
	e2 := errors.New("e2")
	e3 := errors.New("e3")
	var m1 MakeActor = func([]byte) (Actor, error) { return nil, e1 }
	var m2 MakeActor = func([]byte) (Actor, error) { return nil, e2 }
	var m3 MakeActor = func([]byte) (Actor, error) { return nil, e3 }

	equal := func(t testing.TB, want error, got MakeActor) {
		t.Helper()
		_, err := got(nil)
		assert.ErrorIs(t, err, want)
	}

	r := newMakeActorRegistry()

	// Set one
	_, ok := r.Get(n1)
	assert.False(t, ok)

	update := r.Set(n1, m1)
	assert.False(t, update)

	got1, ok := r.Get(n1)
	assert.True(t, ok)
	equal(t, e1, got1)

	// Set second
	update = r.Set(n2, m2)
	assert.False(t, update)

	got1, ok = r.Get(n1)
	assert.True(t, ok)
	equal(t, e1, got1)

	got2, ok := r.Get(n2)
	assert.True(t, ok)
	equal(t, e2, got2)

	// Overwrite first
	update = r.Set(n1, m3)
	assert.True(t, update)

	got1, ok = r.Get(n1)
	assert.True(t, ok)
	equal(t, e3, got1)

	got2, ok = r.Get(n2)
	assert.True(t, ok)
	equal(t, e2, got2)
}
