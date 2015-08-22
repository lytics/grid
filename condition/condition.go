package condition

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

func ActorRunning(etcd *etcd.Client, gridname, pattern string, expected int) (<-chan bool, chan<- bool) {
	done := make(chan bool)
	exit := make(chan bool)
	go func() {
		defer close(done)
		watchpath := fmt.Sprintf("/%v/tasks/", gridname)
		for {
			select {
			case <-exit:
				return
			default:
				time.Sleep(2 * time.Second)
				cnt := 0
				res, err := etcd.Get(watchpath, false, false)
				if err != nil {
					log.Printf("error: watching: %v, error: %v", watchpath, err)
					continue
				}
				if res.Node == nil {
					continue
				}
				for _, n := range res.Node.Nodes {
					if strings.Contains(n.Key, pattern) {
						cnt++
					}
				}
				if cnt == expected {
					return
				}
			}
		}
	}()
	return done, exit
}
