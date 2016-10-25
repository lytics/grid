package balancer

import (
	crypto "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

var (
	ErrInsufficientEntropy = errors.New("insufficient entropy")
)

const (
	MembershipInterval = 2 * time.Second
)

type ownerValue struct {
	Node string `json:"node"`
}

func New(project, nodeid string, c *etcd.Client) (*Balancer, error) {
	seed := make([]byte, 8)
	n, err := crypto.Read(seed)
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, ErrInsufficientEntropy
	}
	r := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(seed))))
	l := log.New(os.Stderr, "", log.LstdFlags)

	return &Balancer{
		c:                       c,
		r:                       r,
		mu:                      new(sync.RWMutex),
		exit:                    make(chan bool),
		nodeid:                  nodeid,
		project:                 project,
		Logger:                  l,
		CheckMembershipInterval: MembershipInterval,
	}, nil
}

type Balancer struct {
	c                       *etcd.Client
	r                       *rand.Rand
	mu                      *sync.RWMutex
	exit                    chan bool
	nodeid                  string
	nodeidx                 uint64
	project                 string
	members                 int
	Logger                  *log.Logger
	CheckMembershipInterval time.Duration

	taskids []string
}

func (b *Balancer) Init(bc metafora.BalancerContext) {
	b.findMembers()
	// Start background check for memebers.
	go func() {
		timer := time.NewTimer(b.CheckMembershipInterval)
		defer timer.Stop()
		for {
			select {
			case <-b.exit:
				return
			case <-timer.C:
				b.findMembers()
				timer.Reset(b.CheckMembershipInterval)
			}
		}
	}()
}

func (b *Balancer) CanClaim(task metafora.Task) (time.Time, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.members == 0 {
		return time.Now().Add(time.Duration(2) * b.CheckMembershipInterval), false
	}

	if claimable(task.ID(), b.nodeidx, b.members) {
		b.taskids = append(b.taskids, task.ID())
		return time.Now(), true
	}

	return time.Time{}, false
}

func claimable(tid string, nodeidx uint64, members int) bool {
	hash := fnv.New64a()
	hash.Write([]byte(tid))
	taskHash := hash.Sum64()
	if taskHash%uint64(members) == nodeidx {
		return true
	}
	return false
}

func (b *Balancer) Balance() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	unwanted := []string{}
	wanted := []string{}
	for _, tid := range b.taskids {
		//if findMembers changes the hashes, we should release tasks that aren't ours
		if claimable(tid, b.nodeidx, b.members) {
			wanted = append(wanted, tid)
		} else {
			unwanted = append(unwanted, tid)
		}
	}
	b.taskids = wanted

	return unwanted
}

func (b *Balancer) Stop() {
	select {
	case <-b.exit:
	default:
		close(b.exit)
	}
}

func (b *Balancer) findMembers() {

	// Get initial set of members.
	res, err := b.c.Get(b.nodePath(), false, true)
	if err != nil {
		b.Logger.Printf("balancer: failed to find members: %v", err)
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.members = 0
	if res != nil && res.Node != nil {
		baseNodeId := path.Base(b.nodeid)
		nodes := make([]string, len(res.Node.Nodes))
		for i, n := range res.Node.Nodes {
			nodes[i] = path.Base(n.Key)
		}
		b.members = len(res.Node.Nodes)
		sort.Strings(nodes)
		for i, n := range nodes {
			if n == baseNodeId {
				b.nodeidx = uint64(i)
			}
		}
	}
}

func (b *Balancer) taskPath() string {
	return fmt.Sprintf("/%v/tasks", b.project)
}

func (b *Balancer) nodePath() string {
	return fmt.Sprintf("/%v/nodes", b.project)
}

// ownedTasks ids plus total task count across whole cluster,
// or error if one occured.
func (b *Balancer) ownedTasks() ([]string, int, error) {

	// Owned task ids.
	owned := []string{}

	// Get nodes.
	res, err := b.c.Get(b.nodePath(), false, true)
	if err != nil {
		return owned, 0, err
	}
	if res == nil || res.Node == nil {
		return owned, 0, nil
	}

	// Get tasks.
	res, err = b.c.Get(b.taskPath(), false, true)
	if err != nil {
		return owned, 0, err
	}
	if res == nil || res.Node == nil {
		return owned, 0, nil
	}

	// Total number of tasks in the whole cluster.
	total := 0
	for _, task := range res.Node.Nodes {
		total++
		for _, claim := range task.Nodes {
			if path.Base(claim.Key) == "owner" {
				v := ownerValue{}
				if err := json.Unmarshal([]byte(claim.Value), &v); err == nil {
					if path.Base(v.Node) == b.nodeid {
						owned = append(owned, task.Key)
					}
				}
			}
		}
	}
	return owned, total, nil
}
