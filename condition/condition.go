package condition

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

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
