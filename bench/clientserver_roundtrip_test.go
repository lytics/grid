package bench

import (
	"context"
	"testing"
	"time"

	"github.com/lytics/grid/v3"
)

// go test -run=^BenchmarkClientServerRoundTrip -bench=. -benchmem
// go test -run=^BenchmarkClientServerRoundTrip -bench=. -benchmem -memprofile /tmp/memprofile.out -cpuprofile /tmp/cpuprofile.out

func BenchmarkClientServerRoundTripSmallMsg(b *testing.B) {
	_, client := runPingPongGrid(b)
	b.ResetTimer()
	benchRunner(b, client, requestSmallMsg)
}

func BenchmarkClientServerRoundTripBigStrMsg(b *testing.B) {
	_, client := runPingPongGrid(b)
	b.ResetTimer()
	benchRunner(b, client, requestBigStrMsg)
}

func BenchmarkClientServerRoundTripBigMapBigStrMsg(b *testing.B) {
	_, client := runPingPongGrid(b)
	b.ResetTimer()
	benchRunner(b, client, requestBigMapBigStrMsg)
}

func benchRunner(b *testing.B, client *grid.Client, evtMsg *Event) {
	for n := 0; n < b.N; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := client.Request(ctx, mailboxName, evtMsg)
		cancel()
		if err != nil {
			b.Fatal(err)
		}
		switch response.(type) {
		case *EventResponse:
		default:
			b.Fatalf("ERROR:  wrong type %#v :: %T", response, response)
		}
	}
}
