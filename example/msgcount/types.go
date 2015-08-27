package main

import "encoding/gob"

func init() {
	gob.Register(ResultMsg{})
	gob.Register(DataMsg{})
	gob.Register(DoneMsg{})
}

type Conf struct {
	GridName    string
	MinSize     int
	MinCount    int
	NrProducers int
	NrConsumers int
}

type DoneMsg struct {
	From string
}

type DataMsg struct {
	From string
	Data string
}

type ResultMsg struct {
	Producer string
	Count    int
}

type SendResultMsg struct {
	Producer string
}
