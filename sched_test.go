package grid

import (
	"fmt"
	"testing"
)

func TestFuncSched(t *testing.T) {

	kafkaparts := make(map[string][]uint32)
	kafkaparts["topic1"] = []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	kafkaparts["topic2"] = []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

	ops := make(map[string]*op)
	op1 := &op{n: 11, inputs: []string{"topic1", "topic2"}}
	ops["f1"] = op1
	op2 := &op{n: 7, inputs: []string{"topic1", "topic2"}}
	ops["f2"] = op2

	hosts := make(map[string]bool)
	hosts["host1"] = true
	hosts["host2"] = true
	hosts["host3"] = true

	sched := hostsched(hosts, ops, kafkaparts)

	fmt.Printf("%v\n", sched.PrettyPrint())
}
