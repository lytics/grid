package main

import (
	"math/rand"
	"time"
)

func NewDataMaker(minsize, count int) *datamaker {
	s := int64(0)
	for i := 0; i < 10000; i++ {
		s += time.Now().UnixNano()
	}
	dice := rand.New(rand.NewSource(s))
	size := minsize + dice.Intn(minsize)

	data := make([]string, 1000)
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	makedata := func(n int) string {
		b := make([]rune, n)
		for i := range b {
			b[i] = letters[dice.Intn(len(letters))]
		}
		return string(b)
	}
	for i := 0; i < 1000; i++ {
		data[i] = makedata(dice.Intn(size))
	}

	return &datamaker{
		size:   size,
		count:  count,
		data:   data,
		dice:   dice,
		done:   make(chan bool),
		output: make(chan string),
	}
}

type datamaker struct {
	size   int
	count  int
	data   []string
	dice   *rand.Rand
	done   chan bool
	output chan string
}

func (d *datamaker) Next() <-chan string {
	return d.output
}

func (d *datamaker) Done() <-chan bool {
	return d.done
}

func (d *datamaker) Start(exit <-chan bool) {
	sent := 0
	for {
		select {
		case <-exit:
			return
		default:
			if sent >= d.count {
				close(d.done)
				return
			}
			select {
			case <-exit:
				return
			case d.output <- d.data[d.dice.Intn(1000)]:
				sent++
			}
		}
	}
}
