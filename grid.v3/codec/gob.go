package codec

import (
	"bytes"
	"encoding/gob"
	"io"
)

type GobCodec struct{}

// Marshal returns v as bytes.
func (g *GobCodec) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal parses data into instance v.
func (g *GobCodec) Unmarshal(data []byte, v interface{}) error {
	var buf bytes.Buffer
	n, err := buf.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrUnexpectedEOF
	}
	dec := gob.NewDecoder(&buf)
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// String returns the name of the Codec implementation. The returned
// string will be used as a key and should be uniq.
func (g *GobCodec) String() string {
	return "gobCodec"
}
