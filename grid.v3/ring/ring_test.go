package ring

import (
	"fmt"
	"strconv"
	"testing"
)

func TestByInt(t *testing.T) {
	r := New("test", 10)
	for i := int(0); i < 100; i++ {
		name := r.ByInt(i)
		if name != fmt.Sprintf("test-%v", i%10) {
			t.Fail()
		}
	}
}

func TestByUint32(t *testing.T) {
	r := New("test", 10)
	for i := uint32(0); i < 100; i++ {
		name := r.ByUint32(i)
		if name != fmt.Sprintf("test-%v", i%10) {
			t.Fail()
		}
	}
}

func TestByUint64(t *testing.T) {
	r := New("test", 10)
	for i := uint64(0); i < 100; i++ {
		name := r.ByUint64(i)
		if name != fmt.Sprintf("test-%v", i%10) {
			t.Fail()
		}
	}
}

func TestByHashedBytes(t *testing.T) {
	r := New("test", 10)
	stats := make(map[string]int)
	for i := int(0); i < 10000; i++ {
		name := r.ByHashedBytes([]byte(strconv.Itoa(i)))
		stats[name]++
	}
	for _, v := range stats {
		if v < 950 {
			t.Fail()
		}
	}
}

func TestByHashedString(t *testing.T) {
	r := New("test", 10)
	stats := make(map[string]int)
	for i := int(0); i < 10000; i++ {
		name := r.ByHashedString(strconv.Itoa(i))
		stats[name]++
	}
	for _, v := range stats {
		if v < 950 {
			t.Fail()
		}
	}
}

func TestByRandom(t *testing.T) {
	r := New("test", 10)
	stats := make(map[string]int)
	for i := int(0); i < 10000; i++ {
		name := r.ByRandom()
		stats[name]++
	}
	for _, v := range stats {
		if v < 950 {
			t.Fail()
		}
	}
}
