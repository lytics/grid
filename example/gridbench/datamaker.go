package main

import (
	"math/rand"
	"time"
)

func NewDataMaker(minsize, mincount int) *datamaker {
	s := int64(0)
	for i := 0; i < 10000; i++ {
		s += time.Now().UnixNano()
	}
	dice := rand.New(rand.NewSource(s))
	size := minsize + dice.Intn(minsize)
	count := mincount + dice.Intn(mincount)
	return &datamaker{
		size:    size,
		count:   count,
		dice:    dice,
		done:    make(chan bool),
		output:  make(chan string),
		letters: []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
	}
}

type datamaker struct {
	size    int
	count   int
	dice    *rand.Rand
	done    chan bool
	output  chan string
	letters []rune
}

func (d *datamaker) Next() <-chan string {
	return d.output
}

func (d *datamaker) Done() <-chan bool {
	return d.done
}

func (d *datamaker) Start(exit <-chan bool) {
	defer close(d.output)
	makedata := func(n int) string {
		b := make([]rune, n)
		for i := range b {
			b[i] = d.letters[d.dice.Intn(len(d.letters))]
		}
		return string(b)
	}
	sent := 0
	for {
		select {
		case <-exit:
			return
		default:
			select {
			case <-exit:
				return
			case d.output <- makedata(d.dice.Intn(d.size)):
				sent++
				if sent >= d.count {
					close(d.done)
					return
				}
			}
		}
	}
}
