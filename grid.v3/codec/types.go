package codec

type Codec interface {
	// Marshal returns v as bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal parses data into instance v.
	Unmarshal(data []byte, v interface{}) error
	//This method returns an empty instance of the object used by the codec.
	BlankSlate() interface{}
	// String returns the name of the Codec implementation. The returned
	// string will be used as a key and should be uniq.
	String() string
}
