package condition

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	mapset "github.com/deckarep/golang-set"
)

func ActorJoinExit(etcd *etcd.Client, exit <-chan bool, gridname, pattern string, expected int) <-chan string {
	name := make(chan string)
	go func() {
		defer close(name)
		members := mapset.NewSet()
		watchpath := fmt.Sprintf("/%v/tasks/", gridname)
		for {
			select {
			case <-exit:
				return
			default:
				time.Sleep(2 * time.Second)
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
						a, err := actorNameFromKey(n.Key)
						if err != nil {
							log.Printf("error: %v", err)
						}
						members.Add(a)
					}
				}
				if members.Cardinality() == expected {
					goto POLL
				}
			}
		}
	POLL:
		for {
			select {
			case <-exit:
				return
			default:
				time.Sleep(2 * time.Second)
				res, err := etcd.Get(watchpath, false, false)
				if err != nil {
					log.Printf("error: watching: %v, error: %v", watchpath, err)
					continue
				}
				if res.Node == nil {
					continue
				}
				current := mapset.NewSet()
				for _, n := range res.Node.Nodes {
					if strings.Contains(n.Key, pattern) {
						a, err := actorNameFromKey(n.Key)
						if err != nil {
							log.Printf("error: %v", err)
						}
						current.Add(a)
					}
				}
				for n := range members.Difference(current).Iter() {
					name <- n.(string)
					members.Remove(n)
				}
			}
		}
	}()
	return name
}

func ActorJoin(etcd *etcd.Client, exit <-chan bool, gridname, pattern string, expected int) <-chan bool {
	return pathCount(etcd, exit, gridname, "tasks", pattern, expected)
}

func NodeJoin(etcd *etcd.Client, exit <-chan bool, gridname, pattern string, expected int) <-chan bool {
	return pathCount(etcd, exit, gridname, "nodes", pattern, expected)
}

func pathCount(etcd *etcd.Client, exit <-chan bool, gridname, dir, pattern string, expected int) <-chan bool {
	done := make(chan bool)
	go func() {
		defer close(done)
		watchpath := fmt.Sprintf("/%v/%v/", gridname, dir)
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
	return done
}

func actorNameFromKey(id string) (string, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 4 {
		return "", fmt.Errorf("unknonw key format: %v", id)
	}
	return parts[3], nil
}
