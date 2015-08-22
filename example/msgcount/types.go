package main

import "encoding/gob"

func init() {
	gob.Register(CntMsg{})
	gob.Register(DoneMsg{})
}

type Conf struct {
	NrReaders  int
	NrCounters int
}

type DoneMsg struct {
	From string
}

type CntMsg struct {
	From   string
	Number int
}
