package grid

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestClientCheck(t *testing.T) {
	t.Parallel()
	_, server, client := bootstrapClientTest(t)

	peer := server.registry.Registry()
	resp, err := client.Check(context.Background(), peer)
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}

// NOTE (2022-06) (mh): WaitUntilServing() is tested via bootstrapClientTest().
// func TestClientWaitUntilServing(t *testing.T) {}
