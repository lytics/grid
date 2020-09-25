package bench

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

// go test -run=^BenchmarkClientServerRoundTrip -bench=. -benchmem
// go test -run=^BenchmarkClientServerRoundTrip -bench=. -benchmem -memprofile /tmp/memprofile.out -cpuprofile /tmp/cpuprofile.out

func BenchmarkClientServerRoundTripSmallMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)
	wg.Wait()
	benchRunner(b, logger, requestSmallMsg)
}

func BenchmarkClientServerRoundTripBigStrMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)
	wg.Wait()
	benchRunner(b, logger, requestBigStrMsg)
}

func BenchmarkClientServerRoundTripBigMapBigStrMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)
	wg.Wait()
	benchRunner(b, logger, requestBigMapBigStrMsg)
}

func BenchmarkClientServerRoundTrip4MBStrMsg(b *testing.B) {
	logger := log.New(os.Stderr, "rountrip-bench :: ", log.LstdFlags|log.Lshortfile)
	wg.Wait()

	strBuffer := bytes.NewBufferString("")
	for i := 0; i < 256; i++ {
		strBuffer.WriteString(message4KBStr)
	}
	requestSuperBigStrMsg.Strdata = strBuffer.String()

	benchRunner(b, logger, requestSuperBigStrMsg)
}

func benchRunner(b *testing.B, logger *log.Logger, evtMsg *Event) {
	ctx := context.Background()
	wgStart := &sync.WaitGroup{}
	wgStart.Add(1)
	wgDone := &sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wgDone.Add(1)
		go func(ii int) {
			wgStart.Wait()
			defer wgDone.Done()

			mailboxName := fmt.Sprintf("%s-%d", mailboxNamePrefix, ii)

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
		}(i)
	}
	wgStart.Done()
	b.ResetTimer()
	wgDone.Wait()
	b.StopTimer()
}
