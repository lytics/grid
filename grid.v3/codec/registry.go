package codec

import (
	"reflect"
	"sync"
)

var registry *codecRegistry = &codecRegistry{reg: map[string]Codec{}} //shared registry

type CodecRegistry interface {
	Register(v interface{}, codec Codec)
	GetCodec(v interface{}) (Codec, bool)
	GetCodecName(name string) (Codec, bool)
}

func Registry() CodecRegistry {
	return registry
}

type codecRegistry struct {
	reg   map[string]Codec
	mutex sync.RWMutex
}

func (cr *codecRegistry) Register(v interface{}, codec Codec) {
	n := Name(v)
	//fmt.Println("reg:", n)
	cr.mutex.Lock()
	defer cr.mutex.Unlock()
	cr.reg[n] = codec
}

func (cr *codecRegistry) GetCodec(v interface{}) (Codec, bool) {
	return cr.GetCodecName(Name(v))
}

func (cr *codecRegistry) GetCodecName(n string) (Codec, bool) {
	//fmt.Println("get:", n)
	cr.mutex.RLock()
	defer cr.mutex.RUnlock()
	c, ok := cr.reg[n]
	return c, ok
}

func Name(v interface{}) string {
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
