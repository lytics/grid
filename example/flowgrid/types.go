package main

import (
	"encoding/gob"
	"log"
	"time"

	"github.com/lytics/grid"
)

func init() {
	gob.Register(ResultMsg{})
	gob.Register(DataMsg{})
}

type Conf struct {
	GridName    string
	MsgSize     int
	MsgCount    int
	NrProducers int
	NrConsumers int
}

type DataMsg struct {
	Producer string
	Data     string
}

type ResultMsg struct {
	Producer string
	From     string
	Count    int
	Duration float64
}

type Chaos struct {
	roll chan bool
	stop chan bool
	C    <-chan bool
}

func NewChaos(name string) *Chaos {
	stop := make(chan bool)
	roll := make(chan bool, 1)
	go func() {
		dice := grid.NewSeededRand()
		delay := time.Duration(30+dice.Intn(600)) * time.Second
		ticker := time.NewTicker(delay)
		happen := ticker.C
		for {
			select {
			case <-stop:
				ticker.Stop()
				return
			case <-happen:
				ticker.Stop()
				delay := time.Duration(30+dice.Intn(600)) * time.Second
				log.Printf("%v: CHAOS", name)
				ticker = time.NewTicker(delay)
				happen = ticker.C
				select {
				case <-stop:
					ticker.Stop()
				case roll <- true:
				}
			}
		}
	}()
	return &Chaos{stop: stop, roll: roll, C: roll}
}

func (c *Chaos) Stop() {
	close(c.stop)
}
