package balancer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/bmizerany/assert"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/lio/src/testutil/testconfig"
	"github.com/lytics/metafora"
)

var ErrNotImplemented = errors.New("not implemented")

func TestInit(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		nodes = 4
	)
	b, bc, project := testSetup(t)

	// Create nodes.
	for i := 0; i < nodes; i++ {
		_, err := b.c.Create(fmt.Sprintf("/%v/nodes/node-%v", project, i), "", ttl)
		assert.Equal(t, nil, err)
	}

	// After initialize is called, the memebrs should
	// match the expected number of memebers created.
	b.Init(bc)
	assert.Equal(t, nodes, b.members)
	b.Stop()
}

func TestErrorWithInit(t *testing.T) {
	b, bc, _ := testSetup(t)

	// No data is entered into etcd, this should cause
	// an error and make the members count 0.

	b.Init(bc)
	assert.Equal(t, 0, b.members)
	b.Stop()
}

func TestMembersUpdate(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		nodes = 4
	)
	b, bc, project := testSetup(t)
	b.CheckMembershipInterval = 500 * time.Millisecond

	// Create nodes.
	for i := 0; i < nodes; i++ {
		_, err := b.c.Create(fmt.Sprintf("/%v/nodes/node-%v", project, i), "", ttl)
		assert.Equal(t, nil, err)
	}

	// After initialize is called, the memebrs should
	// match the expected number of memebers created.
	b.Init(bc)
	assert.Equal(t, nodes, b.members)

	// Delete one node.
	_, err := b.c.Delete(fmt.Sprintf("/%v/nodes/node-%v", project, 0), true)
	assert.Equal(t, nil, err)

	// There should be one less member.
	time.Sleep(time.Duration(3) * b.CheckMembershipInterval)
	assert.Equal(t, nodes-1, b.members)
}

func TestCanClaim(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		tasks = 1000
		nodes = 4
	)
	b, bc, project := testSetup(t)

	// Create nodes.
	for i := 0; i < nodes; i++ {
		_, err := b.c.Create(fmt.Sprintf("/%v/nodes/node-%v", project, i), "", ttl)
		assert.Equal(t, nil, err)
	}

	// Init needs to be called before CanClaim.
	b.Init(bc)

	// CanClaim should operate randomly, X tasks with Y members
	// should, +- some difference, give X/Y "yes" responses.
	cnt := 0
	for i := 0; i < 1000; i++ {
		if _, can := b.CanClaim(&TestRunningTask{name: fmt.Sprintf("task-%v", i)}); can {
			cnt++
		}
	}
	assert.T(t, float64(cnt) > 0.85*(tasks/nodes))
	assert.T(t, float64(cnt) < 1.15*(tasks/nodes))

	b.Stop()
}

func TestZeroMembersWithCanClaim(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		tasks = 1000
		nodes = 4
	)
	b, bc, _ := testSetup(t)

	// Init needs to be called before CanClaim.
	b.Init(bc)

	// With zero members, the balancer should not claim anything.
	next, can := b.CanClaim(&TestRunningTask{name: fmt.Sprintf("task-%v", 0)})
	assert.Equal(t, false, can)
	assert.T(t, time.Now().Add(time.Duration(2)*b.CheckMembershipInterval).After(next))

	b.Stop()
}

func TestStopTwice(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		tasks = 1000
		nodes = 4
	)
	b, bc, _ := testSetup(t)

	// Stop twice.
	b.Init(bc)
	b.Stop()
	b.Stop()
}

func TestRealTaskNamesWithBalance(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		tasks = 100
		nodes = 4
	)
	b, bc, project := testSetup(t)

	// Create nodes.
	for i := 0; i < nodes; i++ {
		_, err := b.c.Create(fmt.Sprintf("/%v/nodes/node-%v", project, i), "", ttl)
		assert.Equal(t, nil, err)
	}

	// Create tasks.
	owned := 0
	for i, task := range realTasks[:tasks] {
		_, err := b.c.CreateDir(fmt.Sprintf("/%v/tasks/%v", project, task), ttl)
		assert.Equal(t, nil, err)
		// Make this node the owner of more than
		// the expected number of tasks.
		if i < tasks/(nodes-1) {
			owned++
			body, err := json.Marshal(&ownerValue{Node: "testing"})
			assert.Equal(t, nil, err)
			_, err = b.c.Create(fmt.Sprintf("/%v/tasks/%v/owner", project, task), string(body), ttl)
			assert.Equal(t, nil, err)
		}
	}

	// Expected number of numbers of tasks that
	// should be released after balancing.
	expected := owned - (tasks / nodes)

	b.Init(bc)
	unwanted := b.Balance()
	assert.Equal(t, expected, len(unwanted))
	b.Stop()
}

func TestFewerThanExpectedTasksBalance(t *testing.T) {
	const (
		ttl   = 5000 // Milliseconds
		nodes = 4
		tasks = 100
	)
	b, bc, project := testSetup(t)

	// Create nodes.
	for i := 0; i < nodes; i++ {
		_, err := b.c.Create(fmt.Sprintf("/%v/nodes/node-%v", project, i), "", ttl)
		assert.Equal(t, nil, err)
	}

	// Create tasks.
	owned := 0
	for i, task := range realTasks[:tasks] {
		_, err := b.c.CreateDir(fmt.Sprintf("/%v/tasks/%v", project, task), ttl)
		assert.Equal(t, nil, err)
		// Make this node the owner of less than
		// the expected number of tasks.
		if i < tasks/(nodes+1) {
			owned++
			body, err := json.Marshal(&ownerValue{Node: "testing"})
			assert.Equal(t, nil, err)
			_, err = b.c.Create(fmt.Sprintf("/%v/tasks/%v/owner", project, task), string(body), ttl)
			assert.Equal(t, nil, err)
		}
	}

	b.Init(bc)
	unwanted := b.Balance()
	assert.Equal(t, 0, len(unwanted))
	b.Stop()
}

func testSetup(t *testing.T) (*Balancer, metafora.BalancerContext, string) {
	c := etcd.NewClient(testconfig.Config.WorkEtcdHosts)
	assert.Equal(t, true, c.SyncCluster())
	err := c.SetConsistency(etcd.STRONG_CONSISTENCY)
	assert.Equal(t, nil, err)

	project := fmt.Sprintf("balancer-%v", rand.Int())

	bc := &TestBalancerContext{}

	b, err := New(project, "testing", c)
	assert.Equal(t, nil, err)
	b.Logger = log.New(&bytes.Buffer{}, "", log.LstdFlags)

	return b, bc, project
}

type TestRunningTask struct {
	name string
}

func (t *TestRunningTask) ID() string {
	return t.name
}

func (t *TestRunningTask) Task() metafora.Task {
	return t
}

func (t *TestRunningTask) Started() time.Time {
	panic(ErrNotImplemented.Error())
}

func (t *TestRunningTask) Stopped() time.Time {
	panic(ErrNotImplemented.Error())
}

func (t *TestRunningTask) Handler() metafora.Handler {
	panic(ErrNotImplemented.Error())
}

type TestBalancerContext struct{}

func (bc *TestBalancerContext) Tasks() []metafora.RunningTask {
	return nil
}
