package grid

import (
	"bytes"
	"fmt"
	"sort"
)

func (fi *FuncInst) PrettyPrint() string {
	topics := make([]string, 0)
	for topic, _ := range fi.topicslices {
		topics = append(topics, topic)
	}

	sort.Strings(topics)

	var buf bytes.Buffer
	buf.Write([]byte(fmt.Sprintf("%v #%v: ", fi.fname, fi.i)))
	for i, topic := range topics {
		buf.Write([]byte(fmt.Sprintf("%v", fi.topicslices[topic])))
		if i < len(topics)-1 {
			buf.Write([]byte(", "))
		}
	}

	return buf.String()
}

func (hs HostSched) PrettyPrint() string {
	hosts := make([]string, len(hs))
	for host, _ := range hs {
		hosts = append(hosts, host)
	}

	sort.Strings(hosts)

	var buf bytes.Buffer
	for host, finsts := range hs {
		for _, finst := range finsts {
			buf.Write([]byte(fmt.Sprintf("%v: %v\n", host, finst.PrettyPrint())))
		}
	}

	return buf.String()
}
