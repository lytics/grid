package bench

import (
	"testing"

	"github.com/lytics/grid/codec"
)

// Run using go test -bench=^BenchmarkCodec -benchmem #   -memprofile /tmp/memprofile.tmp.out -cpuprofile /tmp/cpuprofile.tmp.out
func BenchmarkCodecMarshalBigMapBigStrMsg(b *testing.B) {
	wg.Wait() // we're only waiting here, so we can reset the timers and not include the TestMain...

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := codec.Marshal(requestBigMapBigStrMsg)
		if err != nil {
			b.Fatalf("marshal error: %v", err)
		}
	}
}

func BenchmarkCodecUnmarshalBigMapBigStrMsg(b *testing.B) {
	wg.Wait() // we're only waiting here, so we can reset the timers and not include the TestMain...

	typ, buf, err := codec.Marshal(requestBigMapBigStrMsg)
	if err != nil {
		b.Fatalf("marshal error: %v", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := codec.Unmarshal(buf, typ)
		if err != nil {
			b.Fatalf("marshal error: %v", err)
		}
	}
}
