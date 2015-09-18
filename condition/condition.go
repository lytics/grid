package condition

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/grid/ring"
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
	Rejoin() error
	Exit() error
	Alive() error
}

type JoinWatch interface {
	WatchJoin() <-chan bool
	WatchExit() <-chan bool
	WatchError() <-chan error
	Stop()
}

type CountWatch interface {
	WatchUntil(count int) <-chan bool
	WatchCount() <-chan int
	WatchError() <-chan error
	Stop()
}

type NameWatch interface {
	WatchUntil(name ...interface{}) <-chan bool
	WatchNames() <-chan []string
	WatchError() <-chan error
	Stop()
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
	e     *etcd.Client
	key   string
	ttl   uint64
	exitc chan bool
}

func (j *join) Alive() error {
	_, err := j.e.Update(j.key, "", j.ttl)
	return err
}

func (j *join) Join() error {
	_, err := j.e.Create(j.key, "", j.ttl)
	return err
}

func (j *join) Rejoin() error {
	_, err := j.e.Create(j.key, "", j.ttl)
	if err != nil {
		switch err := err.(type) {
		case *etcd.EtcdError:
			if err.ErrorCode == 105 {
				return nil
			}
		}
	}
	return err
}

func (j *join) Exit() error {
	_, err := j.e.Delete(j.key, false)
	return err
}

func NewJoinWatch(e *etcd.Client, path ...string) JoinWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	joinc := make(chan bool, 1)
	exitc := make(chan bool, 1)
	errorc := make(chan error, 1)
	go func() {
		var res *etcd.Response
		var err error
		var exists bool
		for !exists {
			res, err = e.Get(key, false, false)
			if err != nil {
				switch err := err.(type) {
				case *etcd.EtcdError:
					if err.ErrorCode == 100 {
						ticker := time.NewTicker(5 * time.Second)
						select {
						case <-stopc:
							ticker.Stop()
							return
						case <-ticker.C:
							ticker.Stop()
						}
					}
				default:
					errorc <- err
					return
				}
			} else {
				exists = true
			}
		}

		index := uint64(0)
		if res != nil && res.Node != nil {
			select {
			case joinc <- true:
			default:
			}
			index = res.Node.ModifiedIndex
		}

		watch := make(chan *etcd.Response)
		go e.Watch(key, index, false, watch, stopc)
		for {
			select {
			case <-stopc:
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

func NewCountWatch(e *etcd.Client, path ...string) CountWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	countc := make(chan int, 1)
	errorc := make(chan error, 1)
	go func() {
		var res *etcd.Response
		var err error
		var exists bool
		for !exists {
			res, err = e.Get(key, false, false)
			if err != nil {
				switch err := err.(type) {
				case *etcd.EtcdError:
					if err.ErrorCode == 100 {
						ticker := time.NewTicker(5 * time.Second)
						select {
						case <-stopc:
							ticker.Stop()
							return
						case <-ticker.C:
							ticker.Stop()
						}
					}
				default:
					errorc <- err
					return
				}
			} else {
				exists = true
			}
		}

		index := uint64(0)
		if res != nil && res.Node != nil && res.Node.Dir {
			select {
			case countc <- len(res.Node.Nodes):
			default:
			}
			index = res.Node.ModifiedIndex
		} else {
			select {
			case countc <- 0:
			default:
			}
		}

		watch := make(chan *etcd.Response)
		go e.Watch(key, index, true, watch, stopc)

		for {
			select {
			case <-stopc:
				return
			case res, open := <-watch:
				if !open {
					select {
					case errorc <- fmt.Errorf("count watch closed unexpectedly"):
					default:
					}
					return
				}
				res, err := e.Get(key, false, false)
				if err != nil {
					switch err := err.(type) {
					case *etcd.EtcdError:
						if err.ErrorCode == 100 {
							// Do nothing.
						}
					default:
						errorc <- err
						return
					}
				}
				if res != nil && res.Node != nil && res.Node.Dir {
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

func NewNameWatch(e *etcd.Client, path ...string) NameWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	namesc := make(chan []string, 1)
	errorc := make(chan error, 1)
	go func() {
		var res *etcd.Response
		var err error
		var exists bool
		for !exists {
			res, err = e.Get(key, false, false)
			if err != nil {
				switch err := err.(type) {
				case *etcd.EtcdError:
					if err.ErrorCode == 100 {
						ticker := time.NewTicker(5 * time.Second)
						select {
						case <-stopc:
							ticker.Stop()
							return
						case <-ticker.C:
							ticker.Stop()
						}
					}
				default:
					errorc <- err
					return
				}
			} else {
				exists = true
			}
		}

		index := uint64(0)
		if res != nil && res.Node != nil && res.Node.Dir {
			names := make([]string, 0, len(res.Node.Nodes))
			for _, n := range res.Node.Nodes {
				names = append(names, n.Key)
			}
			select {
			case namesc <- names:
			default:
			}
			index = res.Node.ModifiedIndex
		} else {
			select {
			case namesc <- nil:
			default:
			}
		}

		watch := make(chan *etcd.Response)
		go e.Watch(key, index, true, watch, stopc)

		for {
			select {
			case <-stopc:
				return
			case res, open := <-watch:
				if !open {
					select {
					case errorc <- fmt.Errorf("name watch closed unexpectedly"):
					default:
					}
					return
				}
				res, err := e.Get(key, false, false)
				if err != nil {
					switch err := err.(type) {
					case *etcd.EtcdError:
						if err.ErrorCode == 100 {
							// Do nothing.
						}
					default:
						errorc <- err
						return
					}
				}
				if res != nil && res.Node != nil && res.Node.Dir {
					names := make([]string, 0, len(res.Node.Nodes))
					for _, n := range res.Node.Nodes {
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
