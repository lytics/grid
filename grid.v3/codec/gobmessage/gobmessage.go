package gobmessage

import "encoding/gob"

type Person struct {
	Name   string
	Email  string
	Phones []*PhoneNumber
}

type PhoneNumber struct {
	Number    string
	PhoneType PhoneType
}

type PhoneType int

const (
	Cell PhoneType = 0
	Home PhoneType = 1
	Work PhoneType = 2
)

func init() {
	gob.Register(Person{})
	gob.Register(PhoneNumber{})
	gob.Register(PhoneType(0))
}
