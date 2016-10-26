package grid

import "testing"

func TestNewSeededRand(t *testing.T) {
	r0 := NewSeededRand()
	r1 := NewSeededRand()

	// Not expected to test the "quality" of
	// the seed, just making sure no obvious
	// mistake was made.
	for i := 0; i < 1000; i++ {
		if r0.Int63() == r1.Int63() {
			t.Fail()
		}
	}
}
