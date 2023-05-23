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
	_ = retry.XWithContext(ctx, 3, time.Second, func(ctx context.Context) error {
		var client healthpb.HealthClient
		client, _, err = c.getHealthClient(ctx, nsReceiver)
		if err != nil {
			return nil
		}

		resp, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}

		return nil
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
	_ = retry.XWithContext(ctx, 3, time.Second, func(ctx context.Context) error {
		var client healthpb.HealthClient
		client, _, err = c.getHealthClient(ctx, nsReceiver)
		if err != nil {
			return nil
		}

		recv, err = client.Watch(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("checking health: %w", err)
	}

	return recv, nil
}

// WaitUntilServing blocks until the peer is serving or the context is done.
// Will retry with exponential backoff.
func (c *Client) WaitUntilServing(ctx context.Context, peer string) error {
	b := newBackoff()
	defer b.Stop()

LOOP:
	for {
		if err := b.Backoff(ctx); err != nil {
			return fmt.Errorf("backing off: %w", err)
		}

		stream, err := c.Watch(ctx, peer)
		if err != nil {
			c.logf("watching peer: %v", err)
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

func (c *Client) getHealthClient(ctx context.Context, nsReceiver string) (healthpb.HealthClient, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cc, id, err := c.getCCLocked(ctx, nsReceiver)
	if err != nil {
		return nil, id, err
	}
	return cc.health, id, nil
}
