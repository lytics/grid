package bench

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
	// "github.com/gogo/protobuf/proto"
)

// go test -run=^BenchmarkClientServerRoundTrip -bench=. -benchmem
// go test -run=^BenchmarkClientServerRoundTrip -bench=. -benchmem -memprofile /tmp/memprofile.out -cpuprofile /tmp/cpuprofile.out

func BenchmarkClientServerRoundTripSmallMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)

	wg.Wait()

	b.ResetTimer()
	benchRunner(b, logger, requestSmallMsg)
	b.StopTimer()
}

func BenchmarkClientServerRoundTripBigStrMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)

	wg.Wait()

	b.ResetTimer()
	benchRunner(b, logger, requestBigStrMsg)
	b.StopTimer()
}

func BenchmarkClientServerRoundTripBigMapBigStrMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)

	wg.Wait()

	b.ResetTimer()
	benchRunner(b, logger, requestBigMapBigStrMsg)
	b.StopTimer()
}

func benchRunner(b *testing.B, logger *log.Logger, evtMsg *Event) {
	// ops := grid.OptionalParm{proto.NewBuffer(make([]byte, 1024))}
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		timeoutC, cancel := context.WithTimeout(ctx, 10*time.Second)
		response, err := client.RequestC(timeoutC, mailboxName, evtMsg)
		cancel()
		successOrDie(logger, err)
		switch response.(type) {
		case *EventResponse:
		default:
			logger.Printf("ERROR:  wrong type %#v :: %T", response, response)
			break
		}
	}
}
