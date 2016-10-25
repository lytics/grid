package balancer

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bmizerany/assert"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
	"github.com/lytics/metafora/m_etcd/testutil"
)

var (
	logger = log.New(os.Stderr, "", log.Lmicroseconds|log.Lshortfile)

	testcounter uint64
)

func init() {
	metafora.SetLogger(logger)
	if testing.Verbose() {
		metafora.SetLogLevel(metafora.LogLevelDebug)
	}
}

func TestBalancerMetafora(t *testing.T) {
	t.Parallel()

	ec, coord1, conf1, project := setupEtcd(t)
	conf2 := conf1.Copy()
	conf2.Name = "testclient2"
	coord2, _ := m_etcd.NewEtcdCoordinator(conf2)

	bc := &TestBalancerContext{}

	h := metafora.SimpleHandler(func(task metafora.Task, stop <-chan bool) bool {
		metafora.Debugf("Starting %s", task.ID())
		<-stop
		metafora.Debugf("Stopping %s", task.ID())
		return false // never done
	})

	// Create two consumers & balancers
	b1, err := New(project, coord1.Name(), ec)
	assert.Equal(t, nil, err)
	b1.CheckMembershipInterval = time.Millisecond * 200
	b1.Logger = logger

	con1, err := metafora.NewConsumer(coord1, h, b1)
	assert.Equal(t, nil, err)

	b2, err := New(project, coord2.Name(), ec)
	assert.Equal(t, nil, err)
	b2.CheckMembershipInterval = time.Millisecond * 200
	b2.Logger = logger

	con2, err := metafora.NewConsumer(coord2, h, b2)
	assert.Equal(t, nil, err)

	// Start the two consumers
	go con1.Run()
	go con2.Run()
	defer con1.Shutdown()
	defer con2.Shutdown()

	time.Sleep(time.Millisecond * 500)
	b1.Init(bc)
	b2.Init(bc)
	time.Sleep(time.Millisecond * 500)
	assert.Equal(t, 2, b1.members)
	assert.Equal(t, 2, b2.members)

	cli := m_etcd.NewClient(conf1.Namespace, conf1.Hosts)

	cli.SubmitTask(m_etcd.DefaultTaskFunc("t1", ""))
	cli.SubmitTask(m_etcd.DefaultTaskFunc("t2", ""))
	cli.SubmitTask(m_etcd.DefaultTaskFunc("t3", ""))
	cli.SubmitTask(m_etcd.DefaultTaskFunc("t4", ""))
	cli.SubmitTask(m_etcd.DefaultTaskFunc("t5", ""))
	cli.SubmitTask(m_etcd.DefaultTaskFunc("t6", ""))

	time.Sleep(2 * time.Second)

	metafora.Infof("con1: %d  con2: %d", len(con1.Tasks()), len(con2.Tasks()))
	assert.Equal(t, 3, len(con1.Tasks()))
	assert.Equal(t, 3, len(con2.Tasks()))

	time.Sleep(2 * time.Second) // Rebalance

	assert.Equal(t, 3, len(con1.Tasks()))
	assert.Equal(t, 3, len(con2.Tasks()))

	// Finally make sure that balancing the other node does nothing
	cli.SubmitCommand(conf2.Name, metafora.CommandBalance())

	time.Sleep(2 * time.Second)

	assert.Equal(t, 3, len(con1.Tasks()))
	assert.Equal(t, 3, len(con2.Tasks()))
}

// setupEtcd should be used for all etcd integration tests. It handles the following tasks:
//  * Skip tests if ETCDTESTS is unset
//  * Create and return an etcd client
//  * Create and return an initial etcd coordinator
//  * Clearing the test namespace in etcd
func setupEtcd(t *testing.T) (*etcd.Client, *m_etcd.EtcdCoordinator, *m_etcd.Config, string) {
	client, hosts := testutil.NewEtcdClient(t)
	// /gridtest/
	project := fmt.Sprintf("balancer-%v", rand.Int())
	recursive := true
	client.Delete("/", recursive)
	conf := m_etcd.NewConfig("testclient1", project, hosts)
	coord, err := m_etcd.NewEtcdCoordinator(conf)
	if err != nil {
		t.Fatalf("Error creating etcd coordinator: %v", err)
	}

	return client, coord, conf, project
}

type testLogger struct {
	prefix string
	*testing.T
}

func (l testLogger) Log(lvl metafora.LogLevel, m string, v ...interface{}) {
	l.T.Log(fmt.Sprintf("%s:[%s] %s", l.prefix, lvl, fmt.Sprintf(m, v...)))
}

type testCoordCtx struct {
	testLogger
	lost chan string
}

func newCtx(t *testing.T, prefix string) *testCoordCtx {
	return &testCoordCtx{
		testLogger: testLogger{prefix: prefix, T: t},
		lost:       make(chan string, 10),
	}
}

func (t *testCoordCtx) Lost(task metafora.Task) {
	t.Log(metafora.LogLevelDebug, "Lost(%s)", task.ID())
	t.lost <- task.ID()
}
