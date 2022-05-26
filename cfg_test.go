package grid

import (
	"testing"
	"time"
)

func TestSetClientCfgDefaults(t *testing.T) {
	t.Parallel()
	cfg := ClientCfg{Namespace: "testing"}

	if cfg.Timeout != 0 {
		t.Fatalf("initial Timeout should be zero value")
	}
	if cfg.PeersRefreshInterval != 0 {
		t.Fatalf("initial PeersRefreshInterval should be zero value")
	}

	setClientCfgDefaults(&cfg)

	if cfg.Timeout != 10*time.Second {
		t.Fatalf("initial Timeout should be 10s")
	}
	if cfg.PeersRefreshInterval != 2*time.Second {
		t.Fatalf("initial PeersRefreshInterval should be 2s")
	}
}

func TestSetServerCfgDefaults(t *testing.T) {
	t.Parallel()
	cfg := ServerCfg{Namespace: "testing"}

	if cfg.Timeout != 0 {
		t.Fatalf("initial Timeout should be zero value")
	}
	if cfg.LeaseDuration != 0 {
		t.Fatalf("initial LeaseDuration should be zero value")
	}

	setServerCfgDefaults(&cfg)

	if cfg.Timeout != 10*time.Second {
		t.Fatalf("initial Timeout should be 10s")
	}
	if cfg.LeaseDuration != 60*time.Second {
		t.Fatalf("initial LeaseDuration should be 60s")
	}
}
