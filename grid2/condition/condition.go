package condition

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/grid/grid2/ring"
)

// State saves JSON encoded state in etcd, using CAS operations and
// does the work of maintaining the etcd RAFT index. Eeach operation
// of fetch, store, remove return a boolean flag indicating if the
// operation was operating on stale data. In the case of store and
// remove, it will cause the operation to fail, and the user must
// call fetch to get the latest version and then retry the operation.
//
//     s := condition.NewState(client, 5 * time.Minute, "path", "of", "state")
//     defer s.Stop()
//
// The path must always start at the root. So if /state/producers/p1
// was the etcd path, then the three path parts would be "state",
// "producers", "p1"
//
// The value saved in state can be of any type that can be JSON
// encoded and decoded.
type State interface {
	// Initialize the state. If it already exists an error will
	// be returned.
	//
	//     err := s.Init(v)
	//     if err != nil {
	//         ... error code ...
	//     }
	Init(v interface{}) error
	// Fetch etcd state into v. A bool value of true indicates
	// that there has been an update to the state performed
	// by someone else.
	Fetch(v interface{}) (bool, error)
	// Store v into etcd state. A bool value of true indicates
	// that there has been an update to the state performed
	// by someone else. Stale operations always produce an
	// error in addition.
	//
	//     stale, err := s.Store(v)
	//     if stale {
	//         err := s.Fetch(v)
	//         ... update view ...
	//     }
	//     if err != nil {
	//         ... error code ...
	//     }
	Store(v interface{}) (bool, error)
	// Remove state from etcd. A bool value of true indicates
	// that there has been an update to the state performed
	// by someone else. State operations always produce an
	// error in addition.
	Remove() (bool, error)
	// Index of the state, this is the etcd RAFT index.
	Index() uint64
	// Clean up resources.
	Stop()
}

// Join is used to indicate that some component has joined an operation.
// In conjunction with the JoinWatch, CountWatch, and NameWatch it can
// be used to coordinate components running on different machines via
// etcd.
//
//     j := condition.NewJoin(client, 30 * time.Second, "path", "to", "join")
//     defer j.Stop()
type Join interface {
	// Join to indicate that caller is present. Returns an error
	// if the component has already joined within the last TTL
	// duration.
	//
	//     err := j.Join()
	//     if err != nil {
	//         ... failed to join ...
	//     }
	Join() error
	// Rejoin or join to indicate that the caller is present. Does
	// not return an error if the component has already joined
	// within the last TTL duration.
	//
	//     err := j.Rejoin()
	//     if err != nil {
	//         ... failed to rejoin ...
	//     }
	Rejoin() error
	// Exit to indicate that the caller is no longer present.
	//
	//     err := j.Exit()
	//     if err != nil {
	//         ... failed to exit ...
	//     }
	Exit() error
	// Alive to indicate that the caller is alive. Must be called
	// with a frequency of less than TTL duration.
	//
	//    for {
	//        select {
	//        case <-ticker.C:
	//            j.Alive()
	//        }
	//    }
	Alive() error
	// Clean up resources.
	Stop()
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
type JoinWatch interface {
	// WatchJoin channel sends true when watched path has joined.
	WatchJoin() <-chan bool
	// WatchExit channel sends true when watched path has exited, or
	// its TTL has expired.
	WatchExit() <-chan bool
	// WatchError channel sends error when an error occurs in the watch.
	// Not reading this channel will deadlock the watch in case of error.
	WatchError() <-chan error
	// Clean up resources.
	Stop()
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
type CountWatch interface {
	// WatchUntil count number of members have joined. The channel
	// is closed when the given count joins.
	WatchUntil(count int) <-chan bool
	// WatchCount sends the current number of members joined.
	WatchCount() <-chan int
	// WatchError channel sends error when an error occurs in the watch.
	// Not reading this channel will deadlock the watch in case of error.
	WatchError() <-chan error
	// Clean up resources.
	Stop()
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
type NameWatch interface {
	// WatchUntil the given names are present. The channel is closed
	// when all members are present. Name can be of type string or
	// ring.Ring. If name is of type ring.Ring then all members of
	// the ring are included in the watch. Both type string and
	// ring.Ring can be mixed.
	WatchUntil(name ...interface{}) <-chan bool
	// WatchNames sends the current names of members joined.
	WatchNames() <-chan []string
	// WatchError channel sends error when an error occurs in the watch.
	// Not reading this channel will deadlock the watch in case of error.
	WatchError() <-chan error
	// Clean up resources.
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

func (s *state) Stop() {
	s.e.Close()
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

func (j *join) Stop() {
	j.e.Close()
}

func NewJoinWatch(e *etcd.Client, path ...string) JoinWatch {
	key := strings.Join(path, "/")
	stopc := make(chan bool)
	joinc := make(chan bool, 1)
	exitc := make(chan bool, 1)
	errorc := make(chan error, 1)
	go func() {
		defer e.Close()

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
			index = res.EtcdIndex
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
					case errorc <- fmt.Errorf("join watch closed unexpectedly: %v", key):
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
		defer e.Close()

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
			index = res.EtcdIndex
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
					case errorc <- fmt.Errorf("count watch closed unexpectedly: %v", key):
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
		defer e.Close()

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
			index = res.EtcdIndex
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
					case errorc <- fmt.Errorf("name watch closed unexpectedly for: %v", key):
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
