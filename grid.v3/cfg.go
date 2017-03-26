package grid

import "time"

// ClientCfg where the only required argument is Namespace,
// other fields with their zero value will receive defaults.
type ClientCfg struct {
	// Namespace of grid.
	Namespace string
	// Timeout for communication with etcd, and internal gossip.
	Timeout time.Duration
	// PeersRefreshInterval for polling list of peers in etcd.
	PeersRefreshInterval time.Duration

	CodecLookup codec.Registry
}

// setClientCfgDefaults for those fields that have their zero value.
func setClientCfgDefaults(cfg *ClientCfg) {
	if cfg.PeersRefreshInterval == 0 {
		cfg.PeersRefreshInterval = 2 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
}

// ServerCfg where the only required argument is Namespace,
// other fields with their zero value will receive defaults.
type ServerCfg struct {
	// Namespace of grid.
	Namespace string
	// DisalowLeadership to prevent leader from running on a node.
	DisalowLeadership bool
	// Timeout for communication with etcd, and internal gossip.
	Timeout time.Duration
	// LeaseDuration for data in etcd.
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
