package codec

// Codec for a particular class of encoding, such
// as protobuf or gob.
type Codec interface {
	// Marshal v into bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal byte data into v.
	Unmarshal(data []byte, v interface{}) error
}
