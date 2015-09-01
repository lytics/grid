package main

import "encoding/gob"

func init() {
	gob.Register(ResultMsg{})
	gob.Register(DataMsg{})
}

type Conf struct {
	GridName    string
	MinSize     int
	MinCount    int
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
