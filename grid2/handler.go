package grid2

import "sync"

type actorhandler struct {
	a        Actor
	g        *grid
	exit     chan bool
	stoponce *sync.Once
}

func newHandler(g *grid, a Actor) *actorhandler {
	return &actorhandler{
		a:        a,
		g:        g,
		exit:     make(chan bool),
		stoponce: new(sync.Once),
	}
}

// ID makes actorhandler a metafora.Task
func (h *actorhandler) ID() string {
	return h.a.ID()
}

// Run + Stop makes actorhandler a metafora.Handler
func (h *actorhandler) Run() bool {
	return h.a.Act(h.g, h.exit)
}

// Stop + Run makes actorhandler a metafora.Handler
func (h *actorhandler) Stop() {
	h.stoponce.Do(func() {
		close(h.exit)
	})
}
