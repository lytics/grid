package grid

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

var bufin bytes.Buffer
var bufout bytes.Buffer

const votefile = "/tmp/testfiles-vote-test.gob"
const electionfile = "/tmp/testfiles-election-test.gob"

func TestWriteoutVote(t *testing.T) {

	tmpfile := votefile
	os.Remove(tmpfile)

	//Write out the message
	enc := NewCmdMesgEncoder(&bufin)
	event := NewWritable("TheMeaningOfLife", nil, newVote(0, "42-candidate", 1, "from-foobar"))
	err := enc.Encode(event.Message())
	if err != nil {
		t.Fatalf("%v", err)
	} else {
		val := make([]byte, bufin.Len())
		bufin.Read(val)
		err = ioutil.WriteFile(tmpfile, val, 0644)
		if err != nil {
			t.Fatalf("%v", err)
		}
	}
}

func TestReadinVote(t *testing.T) {
	tmpfile := votefile
	//Read the message in
	dec := NewCmdMesgDecoder(&bufout)
	msg := dec.New()
	val, err := ioutil.ReadFile(tmpfile)
	if err != nil {
		t.Fatalf("%v", err)
	}
	bufout.Write(val)
	err = dec.Decode(msg)
	if err != nil {
		t.Fatalf("error: decode failed: %v: msg: %v value: %v", err, msg, string(bufout.Bytes()))
	}

	t.Logf("%v", msg)

}

func TestCodingElection(t *testing.T) {

	tmpfile := electionfile
	os.Remove(tmpfile)

	//Write out the message
	enc := NewCmdMesgEncoder(&bufin)
	event := NewWritable("TheMeaningOfLife", nil, newElection(0, "42-candidate", 1))
	err := enc.Encode(event.Message())
	if err != nil {
		t.Fatalf("%v", err)
	} else {
		val := make([]byte, bufin.Len())
		bufin.Read(val)
		err = ioutil.WriteFile(tmpfile, val, 0644)
		if err != nil {
			t.Fatalf("%v", err)
		}
	}

	//Read the message in
	dec := NewCmdMesgDecoder(&bufout)
	msg := dec.New()
	val, err := ioutil.ReadFile(tmpfile)
	if err != nil {
		t.Fatalf("%v", err)
	}
	bufout.Write(val)
	err = dec.Decode(msg)
	if err != nil {
		t.Fatalf("error: decode failed: %v: msg: %v value: %v", err, msg, string(bufout.Bytes()))
	}

	t.Logf("%v", msg)

}
