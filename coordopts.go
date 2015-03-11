package grid

import "time"

// Shared cluster coordination options

// NewCoordOptions returns default coordination options
func NewCoordOptions() *CoordOptions {
	c := &CoordOptions{
		Skew:         20,
		TickMillis:   500,
		HeartTimeout: 6,
		ElectTimeout: 20,
		UnsetEpoch:   0,
	}
	c.PeerTimeout = 2 * (c.ElectTimeout + c.Skew)

	return c
}

type CoordOptions struct {
	Skew            int64
	TickMillis      int64
	HeartTimeout    int64
	ElectTimeout    int64
	PeerTimeoutMult int64
	UnsetEpoch      int64
	PeerTimeout     int64
}

func (v *CoordOptions) TickDuration() time.Duration {
	return time.Duration(v.TickMillis * int64(time.Millisecond))
}
