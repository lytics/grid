package codec

import (
	"bytes"
	"encoding/gob"
)

type gobCodec string

const Gob = gobCodec("codec: gob")

func (pc gobCodec) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (pc gobCodec) Unmarshal(buf []byte, v interface{}) error {
	r := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(r)
	return dec.Decode(v)
}
