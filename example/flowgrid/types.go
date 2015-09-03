package main

import (
	"encoding/gob"
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
	dice *rand.Rand
}

func NewChaos() *Chaos {
	return &Chaos{dice: newRand()}
}

func (c *Chaos) Roll() bool {
	n := c.dice.Intn(100)
	if n == 10 {
		panic("chaos happened")
	}
	if n < 20 {
		return true
	}
	return false
}
