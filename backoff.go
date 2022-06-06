package grid

import (
	"context"
	"time"
)

type backoff struct {
	timer *time.Timer
	d     time.Duration
}

func newBackoff() *backoff {
	return &backoff{timer: time.NewTimer(0)}
}

func (b *backoff) Backoff(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.timer.C:
		b.bumpd()
		b.timer.Reset(b.d)
		return nil
	}
}

func (b *backoff) bumpd() {
	switch b.d {
	case 0:
		b.d = 125 * time.Millisecond
	case 64 * time.Second:
	default:
		b.d = b.d * 2
	}
}

func (b *backoff) Stop() {
	b.timer.Stop()
}
