package grid

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/lytics/grid/v3/testetcd"
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

	m1 := &GRPCMailbox{name: "m1"}
	m2 := &GRPCMailbox{name: "m2"}
	m3 := &GRPCMailbox{name: "m3"}

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

			msl := make([]*GRPCMailbox, n)
			for i := 0; i < n; i++ {
				name := strconv.Itoa(i)
				m := GRPCMailbox{name: name}
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

// TestMailboxRegistryConcurrent tests that mailboxRegistry
// is safe to use concurrently. It relies on the -race test flag
// to detect races: the test itself has no assertions.
func TestMailboxRegistryConcurrent(t *testing.T) {
	t.Parallel()

	// NOTE (2022-01) (mh): Only doing one combo for sanity.
	// Could test performance/correctness at higher contention.
	// Leaving as a table-test.

	//numWorkersSet contains all the different numWorkers we want to test.
	numWorkersSet := []int{1}
	//numKeysSet contains all the different number of different keys we want to test.
	numKeysSet := []int{1}

	for _, numWorkers := range numWorkersSet {
		numWorkers := numWorkers
		for _, numKeys := range numKeysSet {
			numKeys := numKeys
			t.Run(fmt.Sprintf("%v-%v", numWorkers, numKeys), func(t *testing.T) {
				t.Parallel()

				r := newMailboxRegistry()

				var wg sync.WaitGroup
				for i := 0; i < numKeys; i++ {
					name := strconv.Itoa(i)

					// Add methods you want to test here
					// NOTE (2022-01) (mh): Adding here to reduce boilerplate.
					ops := []func(){
						func() { r.Get(name) },
						func() { r.Set(name, new(GRPCMailbox)) },
						func() { r.Delete(name) },
						func() { r.Size() },
						func() {
							for _, m := range r.R() {
								_ = m
							}
						},
						func() {
							reg := r.R()
							reg[name] = new(GRPCMailbox)
						},
					}

					for j := 0; j < numWorkers; j++ {
						wg.Add(len(ops))
						for _, op := range ops {
							op := op
							go func() {
								defer wg.Done()
								op()
							}()
						}
					}
				}

				wg.Wait()
			})
		}
	}
}

func TestMailboxClose(t *testing.T) {
	t.Parallel()
	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())

	s, err := NewServer(etcd, ServerCfg{Namespace: newNamespace(t)})
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.Serve(lis); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(func() { <-done })
	t.Cleanup(s.Stop)
	err = s.WaitUntilStarted(context.Background())
	require.NoError(t, err)

	m, err := s.NewMailbox("name", 1)
	require.NoError(t, err)

	select {
	case <-m.C():
		t.Fatal("didn't expect any values")
	default:
		// expected
	}

	err = m.Close()
	require.NoError(t, err)

	select {
	case _, ok := <-m.C():
		assert.False(t, ok)
	default:
		t.Fatal("didn't expect channel to be open")
	}

	// We can close again without panicking
	m.Close()
}
