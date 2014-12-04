package grid

import (
	"bytes"
	"fmt"
	"sort"
)

func (ist *Instance) PrettyPrint() string {
	topics := make([]string, 0)
	for topic, _ := range ist.topicslices {
		topics = append(topics, topic)
	}

	sort.Strings(topics)

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%v #%v: ", ist.fname, ist.i))
	for i, topic := range topics {
		buf.WriteString(fmt.Sprintf("%v", ist.topicslices[topic]))
		if i < len(topics)-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String()
}

func (ps PeerSched) PrettyPrint() string {
	names := make([]string, len(ps))
	for name, _ := range ps {
		names = append(names, name)
	}

	sort.Strings(names)

	var buf bytes.Buffer
	for _, name := range names {
		finsts := ps[name]
		for _, finst := range finsts {
			buf.WriteString(fmt.Sprintf("%v: %v\n", name, finst.PrettyPrint()))
		}
	}

	return buf.String()
}
