package grid

import (
	"time"
)

// ClientCfg where the only required argument is Namespace, 
// other fields with their zero value will receiver defaults.
type ClientCfg struct {
	Namespace     string
	// Etcd configuration.
	Timeout       time.Duration
	LeaseDuration time.Duration
}

// ServerCfg where the only required argument is Namespace, 
// other fields with their zero value will receiver defaults.
type ServerCfg struct {
	Namespace         string
	DisalowLeadership bool
	// Etcd configuration.
	Timeout           time.Duration
	LeaseDuration     time.Duration
}