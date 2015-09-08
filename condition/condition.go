package condition

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type State interface {
	Init(v interface{}) error
	Fetch(v interface{}) (bool, error)
	Store(v interface{}) (bool, error)
	Remove() (bool, error)
	Index() uint64
}

type Join interface {
	Join() error
	Exit() error
	Alive() error
}

type JoinWatch interface {
	WatchJoin() <-chan bool
	WatchExit() <-chan bool
	WatchError() <-chan error
}

type CountWatch interface {
	WatchUntil(count int) <-chan bool
	WatchCount() <-chan int
	WatchError() <-chan error
}

func NewState(e *etcd.Client, ttl time.Duration, path ...string) State {
	realttl := 1 * time.Second
	if ttl.Seconds() > realttl.Seconds() {
		realttl = ttl
	}
	return &state{
		e:   e,
		key: strings.Join(path, "/"),
		ttl: uint64(realttl.Seconds()),
	}
}

type state struct {
	e     *etcd.Client
	key   string
	ttl   uint64
	index uint64
}

func (s *state) Init(v interface{}) error {
	newv, err := json.Marshal(v)
	if err != nil {
		return err
	}
	res, err := s.e.Create(s.key, string(newv), s.ttl)
	if err != nil {
		return err
	}
	s.index = res.EtcdIndex
	return nil
}

func (s *state) Store(v interface{}) (bool, error) {
	newv, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	res, err := s.e.CompareAndSwap(s.key, string(newv), s.ttl, "", s.index)
	if err != nil {
		if err.Error() == "You must give either prevValue or prevIndex." {
			return true, err
		}
		return false, err
	}
	s.index = res.EtcdIndex
	return false, nil
}

func (s *state) Fetch(v interface{}) (bool, error) {
	res, err := s.e.Get(s.key, false, false)
	if err != nil {
		return false, err
	}
	if res.Node == nil {
		return false, nil
	}
	stale := s.index != res.EtcdIndex
	s.index = res.Node.ModifiedIndex
	return stale, json.Unmarshal([]byte(res.Node.Value), v)
}

func (s *state) Remove() (bool, error) {
	res, err := s.e.CompareAndDelete(s.key, "", s.index)
	stale := s.index != res.EtcdIndex
	return stale, err
}

func (s *state) Index() uint64 {
	return s.index
}

func (s *state) String() string {
	return fmt.Sprintf("%d", s.index)
}

func NewJoin(e *etcd.Client, ttl time.Duration, path ...string) Join {
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
	e   *etcd.Client
	key string
	ttl uint64
}

func (j *join) Alive() error {
	_, err := j.e.Update(j.key, "", j.ttl)
	return err
}

func (j *join) Join() error {
	_, err := j.e.Create(j.key, "", j.ttl)
	return err
}

func (j *join) Exit() error {
	_, err := j.e.Delete(j.key, false)
	return err
}

func NewJoinWatch(e *etcd.Client, exit <-chan bool, path ...string) JoinWatch {
	key := strings.Join(path, "/")
	joinc := make(chan bool, 1)
	exitc := make(chan bool, 1)
	errorc := make(chan error, 1)
	go func() {
		res, err := e.Get(key, false, false)
		if err != nil {
			errorc <- err
			return
		}

		if res.Node != nil {
			select {
			case joinc <- true:
			default:
			}
		}

		watchexit := make(chan bool)
		go func() {
			<-exit
			close(watchexit)
		}()

		watch := make(chan *etcd.Response)
		go e.Watch(key, res.EtcdIndex, false, watch, watchexit)
		for {
			select {
			case <-exit:
				return
			case res, open := <-watch:
				if !open {
					select {
					case errorc <- fmt.Errorf("join watch closed unexpectedly"):
					default:
					}
					return
				}
				if res.Node != nil {
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
		joinc:  joinc,
		exitc:  exitc,
		errorc: errorc,
	}
}

type joinwatch struct {
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

func NewCountWatch(e *etcd.Client, exit <-chan bool, path ...string) CountWatch {
	key := strings.Join(path, "/")
	countc := make(chan int, 1)
	errorc := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				res, err := e.Get(key, false, false)
				if err != nil {
					errorc <- err
					return
				}
				if res.Node != nil && res.Node.Dir {
					select {
					case countc <- len(res.Node.Nodes):
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
		mu:         new(sync.Mutex),
		exit:       exit,
		countc:     countc,
		errorc:     errorc,
		untilcache: make(map[int]<-chan bool),
	}
}

type countwatch struct {
	mu         *sync.Mutex
	exit       <-chan bool
	countc     chan int
	errorc     chan error
	untilcache map[int]<-chan bool
}

func (w *countwatch) WatchUntil(count int) <-chan bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if done, ok := w.untilcache[count]; ok {
		return done
	}
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-w.exit:
				return
			case c := <-w.countc:
				if c == count {
					w.mu.Lock()
					close(done)
					delete(w.untilcache, count)
					w.mu.Unlock()
					return
				}
			}
		}
	}()
	w.untilcache[count] = done
	return done
}

func (w *countwatch) WatchCount() <-chan int {
	return w.countc
}

func (w *countwatch) WatchError() <-chan error {
	return w.errorc
}
