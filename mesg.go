package grid

import (
	"encoding/gob"
	"fmt"
	"io"
)

type Event interface {
	Offset() int64
	Key() string
	Message() interface{}
}

type event struct {
	offset  int64
	key     string
	message interface{}
}

func (e *event) Offset() int64 {
	return e.offset
}

func (e *event) Key() string {
	return e.key
}

func (e *event) Message() interface{} {
	return e.message
}

func NewReadable(offset int64, message interface{}) Event {
	return &event{offset: offset, message: message}
}

func NewWritable(key string, message interface{}) Event {
	return &event{key: key, message: message}
}

// CmdMesg is an envelope for more specific messages on the command topic.
type CmdMesg struct {
	Data interface{}
}

func newCmdMesg(data interface{}) *CmdMesg {
	return &CmdMesg{Data: data}
}

func (m *CmdMesg) String() string {
	return fmt.Sprintf("CmdMesg{Data: %v}", m.Data)
}

type coder struct {
	*gob.Encoder
	*gob.Decoder
}

func (c *coder) New() interface{} {
	return &CmdMesg{}
}

func NewCmdMesgDecoder(r io.Reader) Decoder {
	return &coder{nil, gob.NewDecoder(r)}
}

func NewCmdMesgEncoder(w io.Writer) Encoder {
	return &coder{gob.NewEncoder(w), nil}
}

func init() {
	gob.Register(CmdMesg{})
}
