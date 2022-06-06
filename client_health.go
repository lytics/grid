package grid

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lytics/retry"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func (c *Client) Check(ctx context.Context, peer string) (*healthpb.HealthCheckResponse, error) {
	nsReceiver, err := namespaceName(Peers, c.cfg.Namespace, peer)
	if err != nil {
		return nil, fmt.Errorf("namespacing name: %w", err)
	}

	var resp *healthpb.HealthCheckResponse
	retry.X(3, time.Second, func() bool {
		var client healthpb.HealthClient
		client, _, err = c.getHealthClient(ctx, nsReceiver)
		if err != nil {
			return false
		}

		resp, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			return true
		}

		return false
	})
	if err != nil {
		return nil, fmt.Errorf("checking health: %w", err)
	}

	return resp, nil
}

func (c *Client) Watch(ctx context.Context, peer string) (healthpb.Health_WatchClient, error) {
	nsReceiver, err := namespaceName(Peers, c.cfg.Namespace, peer)
	if err != nil {
		return nil, fmt.Errorf("namespacing name: %w", err)
	}

	var recv healthpb.Health_WatchClient
	retry.X(3, time.Second, func() bool {
		var client healthpb.HealthClient
		client, _, err = c.getHealthClient(ctx, nsReceiver)
		if err != nil {
			return false
		}

		recv, err = client.Watch(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			return true
		}

		return false
	})
	if err != nil {
		return nil, fmt.Errorf("checking health: %w", err)
	}

	return recv, nil
}

// WaitUntilServing blocks until the receiver is serving or the context is done.
// Will retry with exponential backoff.
func (c *Client) WaitUntilServing(ctx context.Context, receiver string) error {
	b := newBackoff()
	defer b.Stop()

LOOP:
	for {
		if err := b.Backoff(ctx); err != nil {
			return fmt.Errorf("backing off: %w", err)
		}

		stream, err := c.Watch(ctx, receiver)
		if err != nil {
			c.logf("watching receiver: %v", err)
			continue
		}

		resp := new(healthpb.HealthCheckResponse)
		for resp.Status != healthpb.HealthCheckResponse_SERVING {
			resp, err = stream.Recv()
			if errors.Is(err, io.EOF) {
				c.logf("stream ended, restarting")
				continue LOOP
			}
			if err != nil {
				return fmt.Errorf("receiving: %w", err)
			}
		}

		return nil
	}
}

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

func (c *Client) getHealthClient(ctx context.Context, nsReceiver string) (healthpb.HealthClient, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cc, id, err := c.getCCLocked(ctx, nsReceiver)
	if err != nil {
		return nil, id, err
	}
	return cc.health, id, nil
}
