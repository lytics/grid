package memcondition

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lytics/grid/grid2/condition"
	"github.com/lytics/grid/grid2/ring"
)

var ErrKeyExists = fmt.Errorf("key exists")
var ErrKeyDoesNotExist = fmt.Errorf("key does not exist")

type Node struct {
	Key   string
	Value string
	Index uint64
	Dir   bool
	nodes map[string]*Node
}

func (n *Node) Copy() *Node {
	nodes := make(map[string]*Node)
	for k, v := range n.nodes {
		nodes[k] = v.Copy()
	}
	return &Node{
		Key:   n.Key,
		Value: n.Value,
		Index: n.Index,
		Dir:   n.Dir,
		nodes: nodes,
	}
}

func (n *Node) Nodes() []*Node {
	nodes := make([]*Node, 0, len(n.nodes))
	for _, v := range n.nodes {
		nodes = append(nodes, v)
	}
	return nodes
}

type Response struct {
	Node  *Node
	Index uint64
}

func NewMemory() *Memory {
	return &Memory{mu: new(sync.Mutex), nodes: make(map[string]*Node)}
}

type Memory struct {
	mu    *sync.Mutex
	nodes map[string]*Node
	index uint64
}

func (m *Memory) Get(key string) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.index++
	// The slice 'parts' starts with the empty-string, so depth
	// needs to start at 1.
	parts := strings.Split(key, "/")
	return m.get(m.nodes, parts, 1)
}

func (m *Memory) get(parent map[string]*Node, key []string, depth int) (*Response, error) {
	if len(key)-1 == depth {
		idxkey := key[depth]
		n := parent[idxkey]
		if n != nil {
			n = n.Copy()
		} else {
			return nil, ErrKeyDoesNotExist
		}
		return &Response{Node: n, Index: m.index}, nil
	} else {
		idxkey := key[depth]
		n := parent[idxkey]
		if n == nil || !n.Dir {
			return nil, ErrKeyDoesNotExist
		}
		return m.get(n.nodes, key, depth+1)
	}
}

func (m *Memory) Delete(key string) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.index++
	// The slice 'parts' starts with the empty-string, so depth
	// needs to start at 1.
	parts := strings.Split(key, "/")
	return m.delete(m.nodes, parts, 1)
}

func (m *Memory) delete(parent map[string]*Node, key []string, depth int) (*Response, error) {
	if len(key)-1 == depth {
		idxkey := key[depth]
		n := parent[idxkey]
		if n != nil {
			delete(parent, idxkey)
		} else {
			return nil, ErrKeyDoesNotExist
		}
		return &Response{Index: m.index}, nil
	} else {
		idxkey := key[depth]
		n := parent[idxkey]
		if n == nil || !n.Dir {
			return nil, ErrKeyDoesNotExist
		}
		return m.delete(n.nodes, key, depth+1)
	}
}

func (m *Memory) Create(key, value string) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.index++
	parts := strings.Split(key, "/")
	return m.create(m.nodes, parts, &value, 1, false)
}

func (m *Memory) CreateDir(key string) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.index++
	parts := strings.Split(key, "/")
	r, err := m.create(m.nodes, parts, nil, 1, false)
	return r, err
}

func (m *Memory) create(parent map[string]*Node, key []string, value *string, depth int, update bool) (*Response, error) {
	if len(key)-1 == depth {
		if value == nil {
			idxkey := key[depth]
			nodekey := strings.Join(key[:depth+1], "/")
			if _, ok := parent[idxkey]; ok && !update {
				return nil, ErrKeyExists
			}
			n := &Node{
				Dir:   true,
				Index: m.index,
				nodes: make(map[string]*Node),
				Key:   nodekey,
			}
			parent[idxkey] = n
		} else {
			idxkey := key[depth]
			nodekey := strings.Join(key[:depth+1], "/")
			if _, ok := parent[idxkey]; ok && !update {
				return nil, ErrKeyExists
			}
			n := &Node{
				Index: m.index,
				nodes: make(map[string]*Node),
				Key:   nodekey,
				Value: *value,
			}
			parent[idxkey] = n
		}
		return &Response{Index: m.index}, nil
	} else {
		idxkey := key[depth]
		n := parent[idxkey]
		return m.create(n.nodes, key, value, depth+1, update)
	}
}

func (m *Memory) Update(key, value string) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.index++
	parts := strings.Split(key, "/")
	return m.create(m.nodes, parts, &value, 1, true)
}

func (m *Memory) Describe() string {
	var buf bytes.Buffer
	m.describe(&buf, m.nodes, 0)
	return buf.String()
}

func (m *Memory) describe(buf *bytes.Buffer, tree map[string]*Node, depth int) {
	sorted := make([]string, 0, len(tree))
	for k, _ := range tree {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		pad := make([]rune, depth)
		for i, _ := range pad {
			pad[i] = ' '
		}
		n := tree[k]
		if n.Dir {
			buf.WriteString(string(pad))
			buf.WriteString(n.Key)
			buf.WriteString(":")
			buf.WriteString(strconv.FormatUint(n.Index, 10))
			buf.WriteString("\n")
			m.describe(buf, n.nodes, depth+1)
		} else {
			buf.WriteString(string(pad))
			buf.WriteString(n.Key)
			buf.WriteString(":")
			buf.WriteString(n.Value)
			buf.WriteString(":")
			buf.WriteString(strconv.FormatUint(n.Index, 10))
			buf.WriteString("\n")
		}
	}
}

// Join is used to indicate that some component has joined an operation.
// In conjunction with the JoinWatch, CountWatch, and NameWatch it can
// be used to coordinate components running on different machines via
// etcd.
//
//     j := condition.NewJoin(client, 30 * time.Second, "path", "to", "join")
//     defer j.Stop()
func NewJoin(e *Memory, ttl time.Duration, path ...string) condition.Join {
	realttl := 1 * time.Second
	if ttl.Seconds() > realttl.Seconds() {
		realttl = ttl
	}
	return &join{
		e:   e,
		key: strings.Join(path, "/"),
		ttl: uint64(realttl.Seconds()),
	}
}

type join struct {
	e     *Memory
	key   string
	ttl   uint64
	exitc chan bool
}

func (j *join) Alive() error {
	_, err := j.e.Update(j.key, "")
	return err
}

func (j *join) Join() error {
	_, err := j.e.Create(j.key, "")
	return err
}

func (j *join) Rejoin() error {
	_, err := j.e.Create(j.key, "")
	if err != nil {
		if err == ErrKeyExists {
			return nil
		}
		return err
	}
	return err
}

func (j *join) Exit() error {
	_, err := j.e.Delete(j.key)
	return err
}

func (j *join) Stop() {
}

// JoinWatch watches for joins. It can be used to discover when
// another component was joined or exited.
//
//     w := condition.NewJoinWatch(client, "path", "to", "watch")
//     defer w.Stop()
//
//     for {
//         select {
//         case <- w.WatchJoin():
//             ... partner has joined ...
//         case <- w.WatchExit():
//             ... partner has exited ...
//         case err := <- w.WatchError():
//             ... error code ...
//         }
//     }
func NewJoinWatch(e *Memory, path ...string) condition.JoinWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	joinc := make(chan bool, 1)
	exitc := make(chan bool, 1)
	errorc := make(chan error, 1)
	go func() {
		var res *Response
		var err error
		var exists bool
		for !exists {
			res, err = e.Get(key)
			if err != nil {
				if err == ErrKeyDoesNotExist {
					ticker := time.NewTicker(5 * time.Second)
					select {
					case <-stopc:
						ticker.Stop()
						return
					case <-ticker.C:
						ticker.Stop()
					}
				} else {
					errorc <- err
					return
				}
			} else {
				exists = true
			}
		}
		for {
			select {
			case <-stopc:
				return
			default:
				time.Sleep(5 * time.Second)
				res, err = e.Get(key)
				if res != nil && res.Node != nil {
					select {
					case joinc <- true:
					default:
					}
				} else {
					select {
					case exitc <- true:
					default:
					}
				}
			}
		}
	}()
	return &joinwatch{
		stopc:  stopc,
		joinc:  joinc,
		exitc:  exitc,
		errorc: errorc,
	}
}

type joinwatch struct {
	stopc  chan bool
	joinc  chan bool
	exitc  chan bool
	errorc chan error
}

func (w *joinwatch) WatchJoin() <-chan bool {
	return w.joinc
}

func (w *joinwatch) WatchExit() <-chan bool {
	return w.exitc
}

func (w *joinwatch) WatchError() <-chan error {
	return w.errorc
}

func (w *joinwatch) Stop() {
	close(w.stopc)
}

// CountWatch watches for some number of joins. It can be used to discover
// when a known number of group memebers are present.
//
//     w := condition.NewCountWatch(client, "path", "to", "watch")
//     defer w.Stop()
//
// The path must be to an etcd directory, otherwise the watch will not
// function.
//
//     for {
//         select {
//         case <- w.WatchUntil(10):
//             ... 10 partners are present ...
//         case err := <- w.WatchError():
//             ... error code ...
//         }
//     }
//
// Or
//     for {
//         select {
//         case n := <- w.WatchCount():
//             ... n partners are present ...
//         case err := <- w.WatchError():
//             ... error code ...
//         }
//     }
//
// It is not correct to select from both WatchUntil and WatchCount at
// the same time.
func NewCountWatch(e *Memory, path ...string) condition.CountWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	countc := make(chan int, 1)
	errorc := make(chan error, 1)
	go func() {
		var res *Response
		var err error
		var exists bool
		for !exists {
			res, err = e.Get(key)
			if err != nil {
				if err == ErrKeyDoesNotExist {
					ticker := time.NewTicker(5 * time.Second)
					select {
					case <-stopc:
						ticker.Stop()
						return
					case <-ticker.C:
						ticker.Stop()
					}
				} else {
					errorc <- err
					return
				}
			} else {
				exists = true
			}
		}

		if res != nil && res.Node != nil && res.Node.Dir {
			select {
			case countc <- len(res.Node.Nodes()):
			default:
			}
		} else {
			select {
			case countc <- 0:
			default:
			}
		}

		for {
			select {
			case <-stopc:
				return
			default:
				time.Sleep(5 * time.Second)
				res, err := e.Get(key)
				if err != nil {
					if err == ErrKeyDoesNotExist {
						// Do nothing.
					} else {
						errorc <- err
						return
					}
				}
				if res != nil && res.Node != nil && res.Node.Dir {
					select {
					case countc <- len(res.Node.Nodes()):
					default:
					}
				} else {
					select {
					case countc <- 0:
					default:
					}
				}
			}
		}
	}()
	return &countwatch{
		stopc:  stopc,
		countc: countc,
		errorc: errorc,
	}
}

type countwatch struct {
	stopc  chan bool
	countc chan int
	errorc chan error
}

func (w *countwatch) WatchUntil(count int) <-chan bool {
	done := make(chan bool)
	go func() {
		defer close(done)
		for {
			select {
			case <-w.stopc:
				return
			case c := <-w.countc:
				if c == count {
					return
				}
			}
		}
	}()
	return done
}

func (w *countwatch) WatchCount() <-chan int {
	return w.countc
}

func (w *countwatch) WatchError() <-chan error {
	return w.errorc
}

func (w *countwatch) Stop() {
	close(w.stopc)
}

// NameWatch watches for specific member names to join. It can be used
// to discover when a known group of members is present.
//
//     w := condition.NewNameWatch(client, "path", "to", "watch")
//     defer w.Stop()
//
// The path must be to an etcd directory, otherwise the watch will not
// function.
//
//     for {
//         select {
//         case <- w.WatchUntil("member1", "member2"):
//             ... 2 partners are present ...
//         case err := <- w.WatchError():
//             ... error code ...
//         }
//     }
//
// Or
//     for {
//         select {
//         case names := <- w.WatchNames():
//             ... names of partners currently present ...
//         case err := <- w.WatchError():
//             ... error code ...
//         }
//     }
//
// It is not correct to select from both WatchUntil and WatchCount at
// the same time.
func NewNameWatch(e *Memory, path ...string) condition.NameWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	namesc := make(chan []string, 1)
	errorc := make(chan error, 1)
	go func() {
		var res *Response
		var err error
		var exists bool
		for !exists {
			res, err = e.Get(key)
			if err != nil {
				if err == ErrKeyDoesNotExist {
					ticker := time.NewTicker(5 * time.Second)
					select {
					case <-stopc:
						ticker.Stop()
						return
					case <-ticker.C:
						ticker.Stop()
					}
				} else {
					errorc <- err
					return
				}
			} else {
				exists = true
			}
		}

		if res != nil && res.Node != nil && res.Node.Dir {
			names := make([]string, 0, len(res.Node.Nodes()))
			for _, n := range res.Node.Nodes() {
				names = append(names, n.Key)
			}
			select {
			case namesc <- names:
			default:
			}
		} else {
			select {
			case namesc <- nil:
			default:
			}
		}

		for {
			select {
			case <-stopc:
				return
			default:
				time.Sleep(5 * time.Second)
				res, err := e.Get(key)
				if err != nil {
					if err == ErrKeyDoesNotExist {
						// Do nothing.
					} else {
						errorc <- err
						return
					}
				}
				if res != nil && res.Node != nil && res.Node.Dir {
					names := make([]string, 0, len(res.Node.Nodes()))
					for _, n := range res.Node.Nodes() {
						names = append(names, n.Key)
					}
					select {
					case namesc <- names:
					default:
					}
				} else {
					select {
					case namesc <- nil:
					default:
					}
				}
			}
		}
	}()
	return &namewatch{
		stopc:  stopc,
		namesc: namesc,
		errorc: errorc,
	}
}

type namewatch struct {
	stopc  chan bool
	namesc chan []string
	errorc chan error
}

func (w *namewatch) WatchUntil(name ...interface{}) <-chan bool {
	names := make(map[string]bool)
	for _, n := range name {
		switch n := n.(type) {
		case string:
			names[n] = true
		case ring.Ring:
			for _, def := range n.ActorDefs() {
				names[def.ID()] = true
			}
		}
	}
	done := make(chan bool)
	go func() {
		defer close(done)
		for {
			select {
			case <-w.stopc:
				return
			case ns := <-w.namesc:
				found := 0
				if len(ns) >= len(names) {
					for _, n := range ns {
						r := n
						i := strings.LastIndex(n, "/")
						if i > -1 && i < len(n) {
							r = n[i+1:]
						}
						if names[r] {
							found++
						}
					}
					// Key names in Etcd are unique. Each 'n' in 'ns' is
					// thus unique, so the same string will never be
					// compared twice with the contents of 'names'. If
					// the count of found is the same as the length of
					// 'names' then all expected names were found.
					if found == len(names) {
						return
					}
				}
			}
		}
	}()
	return done
}

func (w *namewatch) WatchNames() <-chan []string {
	return w.namesc
}

func (w *namewatch) WatchError() <-chan error {
	return w.errorc
}

func (w *namewatch) Stop() {
	close(w.stopc)
}
