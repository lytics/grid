package codec

import "github.com/gogo/protobuf/proto"

type protobufCodec string

const Protobuf = protobufCodec("codec: protobuf")

func (pc protobufCodec) Marshal(v interface{}) ([]byte, error) {
	pb := v.(proto.Message)
	return proto.Marshal(pb)
}

func (pc protobufCodec) Unmarshal(buf []byte, v interface{}) error {
	pb := v.(proto.Message)
	return proto.Unmarshal(buf, pb)
}
