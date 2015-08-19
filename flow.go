package grid

import "github.com/coreos/go-etcd/etcd"

type Flow interface {
	Start() error
}

type flow struct {
	g    *grid
	name string
	conf map[string]int
}

func newFlow(name string, g *grid) *flow {
	return &flow{
		name: name,
		g:    g,
		conf: make(map[string]int),
	}
}

func (f *flow) Start() error {
	f.g.mu.Lock()
	defer f.g.mu.Unlock()

	for role, n := range f.conf {
		for part := 0; part < n; part++ {
			t, err := newActorTask(f.name, role, part)
			if err != nil {
				return err
			}
			err = f.g.client.SubmitTask(t)
			if err != nil {
				switch err := err.(type) {
				case *etcd.EtcdError:
					// If the error code is 105, this means the task is
					// already in etcd, which could be possible after
					// a crash or after the actor exits but returns
					// true to be kept as an entry for metafora to
					// schedule again.
					if err.ErrorCode != 105 {
						return err
					}
				default:
					return err
				}
			}
		}
	}
	return nil
}
