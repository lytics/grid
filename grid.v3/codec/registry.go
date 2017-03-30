package codec

import (
	"fmt"
	"reflect"
	"sync"
)

var registry *codecRegistry = &codecRegistry{reg: map[string]Codec{}} //shared registry

type CodecRegistry interface {
	Register(v interface{}, codec Codec)
	GetCodec(v interface{}) Codec
}

func Registry() CodecRegistry {
	return registry
}

type codecRegistry struct {
	reg   map[string]Codec
	mutex sync.Mutex
}

func (cr *codecRegistry) Register(v interface{}, codec Codec) {
	n := name(v)
	fmt.Println("reg:", n)
	cr.mutex.Lock()
	defer cr.mutex.Unlock()
	cr.reg[n] = codec
}

func (cr *codecRegistry) GetCodec(v interface{}) Codec {
	n := name(v)
	fmt.Println("get:", n)
	cr.mutex.Lock() //TODO read lock?
	defer cr.mutex.Unlock()
	return cr.reg[n]
}

func name(v interface{}) string {
	rt := reflect.TypeOf(v)
	name := rt.String()
	// But for named types (or pointers to them), qualify with import path (but see inner comment).
	// Dereference one pointer looking for a named type.
	star := ""
	if rt.Name() == "" {
		if pt := rt; pt.Kind() == reflect.Ptr {
			star = "*"
			rt = pt
		}
	}
	if rt.Name() != "" {
		name = star + rt.PkgPath() + "." + rt.Name()
	}
	return name
}
