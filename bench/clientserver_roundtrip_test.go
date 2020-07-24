package bench

import (
	"log"
	"os"
	"testing"
	"time"
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
	for n := 0; n < b.N; n++ {
		response, err := client.Request(10*time.Second, mailboxName, evtMsg)
		successOrDie(logger, err)
		switch response.(type) {
		case *EventResponse:
		default:
			logger.Printf("ERROR:  wrong type %#v :: %T", response, response)
			break
		}
	}
}
