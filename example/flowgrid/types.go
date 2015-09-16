package main

import (
	"encoding/gob"
	"log"
	"math/rand"
	"time"
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

func newRand() *rand.Rand {
	s := int64(0)
	for i := 0; i < 10000; i++ {
		s += time.Now().UnixNano()
	}
	return rand.New(rand.NewSource(s))
}

type Chaos struct {
	roll chan bool
	stop chan bool
}

func NewChaos(name string) *Chaos {
	stop := make(chan bool)
	roll := make(chan bool, 1)
	go func() {
		dice := newRand()
		delay := time.Duration(30+dice.Intn(600)) * time.Second
		ticker := time.NewTicker(delay)
		happen := ticker.C
		for {
			select {
			case <-stop:
				ticker.Stop()
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
	return &Chaos{stop: stop, roll: roll}
}

func (c *Chaos) Roll() <-chan bool {
	return c.roll
}

func (c *Chaos) Stop() {
	close(c.stop)
}
