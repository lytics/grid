package grid

import (
	"time"
)

// ClientCfg where the only required argument is Namespace,
// other fields with their zero value will receive defaults.
type ClientCfg struct {
	Namespace            string
	PeersRefreshInterval time.Duration
	// Etcd configuration.
	Timeout       time.Duration
	LeaseDuration time.Duration
}

// setClientCfgDefaults for those fields that have their zero value.
func setClientCfgDefaults(cfg *ClientCfg) {
	if cfg.PeersRefreshInterval == 0 {
		cfg.PeersRefreshInterval = 2 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 60 * time.Second
	}
}

// ServerCfg where the only required argument is Namespace,
// other fields with their zero value will receive defaults.
type ServerCfg struct {
	Namespace         string
	DisalowLeadership bool
	// Etcd configuration.
	Timeout       time.Duration
	LeaseDuration time.Duration
}

// setServerCfgDefaults for those fields that have their zero value.
func setServerCfgDefaults(cfg *ServerCfg) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 60 * time.Second
	}
}
