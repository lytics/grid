package ring

import (
	"fmt"
	"strconv"
	"testing"
)

const (
	name = "reader"
)

func TestByInt(t *testing.T) {
	t.Parallel()
	r := New(name, 10)
	for i := int(0); i < 100; i++ {
		name := r.ByInt(i)
		if name != fmt.Sprintf("reader-%v", i%10) {
			t.Fail()
		}
	}
}

func TestByUint32(t *testing.T) {
	t.Parallel()
	r := New(name, 10)
	for i := uint32(0); i < 100; i++ {
		name := r.ByUint32(i)
		if name != fmt.Sprintf("reader-%v", i%10) {
			t.Fail()
		}
	}
}

func TestByUint64(t *testing.T) {
	t.Parallel()
	r := New(name, 10)
	for i := uint64(0); i < 100; i++ {
		name := r.ByUint64(i)
		if name != fmt.Sprintf("reader-%v", i%10) {
			t.Fail()
		}
	}
}

func TestByHashedBytes(t *testing.T) {
	t.Parallel()
	r := New(name, 10)
	stats := make(map[string]int)
	for i := int(0); i < 10000; i++ {
		name := r.ByHashedBytes([]byte(strconv.Itoa(i)))
		stats[name]++
	}
	for _, v := range stats {
		if v < 930 {
			t.Fail()
		}
	}
}

func TestByHashedString(t *testing.T) {
	t.Parallel()
	r := New(name, 10)
	stats := make(map[string]int)
	for i := int(0); i < 10000; i++ {
		name := r.ByHashedString(strconv.Itoa(i))
		stats[name]++
	}
	for _, v := range stats {
		if v < 930 {
			t.Fail()
		}
	}
}

func TestByRandom(t *testing.T) {
	t.Parallel()
	r := New(name, 10)
	stats := make(map[string]int)
	for i := int(0); i < 10000; i++ {
		name := r.ByRandom()
		stats[name]++
	}
	for _, v := range stats {
		if v < 930 {
			t.Fail()
		}
	}
}
