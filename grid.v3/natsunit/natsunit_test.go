package natsunit

import "testing"

func TestEmbeddedNATS(t *testing.T) {
	for i := 0; i < 10; i++ {
		s, err := StartEmbeddedNATS()
		if err != nil {
			t.Fatalf("failed to start nats server: %v", err)
		}
		s.Shutdown()
	}
}
