package codec

import (
	"reflect"
	"sync"
)

type CodecRegistry struct {
	reg   map[string]Codec
	mutex sync.Mutex
}

func (cr *CodecRegistry) Register(v interface{}, codec Codec) {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()
	cr.reg[name(v)] = codec
}

func (cr *CodecRegistry) CodecFor(v interface{}) Codec {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()
	return cr.reg[name(v)]
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

var registry *CodecRegistry = &CodecRegistry{} //default registry

func Registry() *CodecRegistry {
	return registry
}
