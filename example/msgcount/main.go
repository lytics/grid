package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/lytics/grid2"
)

const (
	SendCount  = 10 * 1000 * 1000
	NrReaders  = 20
	NrCounters = 10
)

func init() {
	gob.Register(CntMsg{})
}

func main() {
	runtime.GOMAXPROCS(4)

	g := grid2.New("linkgrid", []string{"http://127.0.0.1:2379"}, []string{"nats://localhost:4222"})
	g.RegisterActor("reader", NrReaders, NewReaderActor)
	g.RegisterActor("counter", NrCounters, NewCounterActor)

	exit, err := g.Start()
	if err != nil {
		log.Fatalf("error: failed to start grid: %v", err)
	}

	f, err := g.NewFlow("aid-12", "reader", "counter")
	if err != nil {
		log.Fatalf("failed to create flow: %v", err)
	}
	err = f.Start()
	if err != nil {
		log.Fatalf("failed to start flow: %T :: %v", err, err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sig:
		log.Printf("Shutting down")
		g.Stop()
	case <-exit:
		log.Printf("Shutting down, grid exited")
	}
}

type CntMsg struct {
	From   string
	Number int
}

func NewReaderActor(id, state string) grid2.Actor {
	return &ReaderActor{id: id, state: state}
}

type ReaderActor struct {
	id    string
	state string
}

func (a *ReaderActor) ID() string {
	return a.id
}

func (a *ReaderActor) Act(c grid2.Conn, exit <-chan bool) bool {
	log.Printf("%v: running", a.id)

	counts := make(map[int]int)
	tx := 0
	for {
		select {
		case <-exit:
			return false
		default:
			if tx < SendCount {
				n, err := c.SendByHashedInt("counter", tx, &CntMsg{From: a.id, Number: tx})
				if err != nil {
					log.Printf("%v: error: %v", a.id, err)
					continue
				}
				tx++
				counts[n]++
			} else {
				var buf bytes.Buffer
				total := 0
				for n, c := range counts {
					buf.WriteString(fmt.Sprintf("   to: counter-%v, sent: %v\n", n, c))
					total += c
				}
				log.Printf("%v: total: %v, counts:\n%v", a.id, total, buf.String())
				return false
			}
		}
	}
}

func NewCounterActor(id, state string) grid2.Actor {
	return &CounterActor{id: id, state: state}
}

type CounterActor struct {
	id    string
	state string
}

func (a *CounterActor) ID() string {
	return a.id
}

func (a *CounterActor) Act(c grid2.Conn, exit <-chan bool) bool {
	log.Printf("%v: running", a.id)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	counts := make(map[string]int)

	ts := time.Now()
	rx := 0
	input := c.Receive()
	for {
		select {
		case <-exit:
			return false
		case <-ticker.C:
			var buf bytes.Buffer
			for n, c := range counts {
				buf.WriteString(fmt.Sprintf("    from: %v, rx: %v\n", n, c))
			}
			log.Printf("%v: data rate: %.2f/sec, counts:\n%v", a.id, float64(rx)/time.Now().Sub(ts).Seconds(), buf.String())
		case m := <-input:
			switch m := m.(type) {
			case CntMsg:
				counts[m.From]++
				rx++
			default:
				log.Printf("%v: unknown message type received: %T", a.id, m)
			}
		}
	}
}

type StringGen struct {
	dice  *rand.Rand
	words []string
}

func NewStringGen(seed []byte, words []string) *StringGen {
	h := fnv.New64()
	h.Write(seed)
	return &StringGen{dice: rand.New(rand.NewSource(time.Now().Unix() + int64(h.Sum64()))), words: words}
}

func (sg *StringGen) Another() string {
	return sg.words[sg.dice.Intn(len(sg.words))]
}

var story = []string{
	"1km",
	"2014",
	"600",
	"a",
	"about",
	"According",
	"accounts",
	"after",
	"aftermath",
	"against",
	"all",
	"Almost",
	"also",
	"amount",
	"an",
	"and",
	"And",
	"answer",
	"apparently",
	"appear",
	"appeared",
	"are",
	"around",
	"arrived",
	"as",
	"As",
	"asked",
	"at",
	"attempts",
	"attention",
	"audit",
	"away",
	"barely",
	"be",
	"been",
	"before",
	"began",
	"behalf",
	"behaviour",
	"between",
	"block",
	"Both",
	"buildings",
	"bureau",
	"business",
	"busy",
	"But",
	"buy",
	"by",
	"Caijing",
	"came",
	"censors",
	"charge",
	"chat",
	"chemicals",
	"China",
	"China’s",
	"Chinese",
	"city",
	"close",
	"commentary",
	"common",
	"communist",
	"company",
	"completel",
	"concerned",
	"conference",
	"conferences",
	"confirmed—it",
	"“contract",
	"contract",
	"contrast",
	"control",
	"cut",
	"cyanide",
	"Daily",
	"damaged",
	"dangerous",
	"day",
	"days",
	"debate",
	"demanding",
	"demonstrations",
	"deserved",
	"did",
	"died",
	"disaster",
	"distance",
	"distrust",
	"dozens",
	"drew",
	"early",
	"ease",
	"embarrassment",
	"environmental",
	"example",
	"exploded",
	"explosions",
	"explosions—and",
	"failed",
	"failures",
	"fallen",
	"fate",
	"fifth",
	"fighting",
	"fire",
	"firefighters—firemen",
	"firemen",
	"firemen”:",
	"fires",
	"first",
	"for",
	"former",
	"foul",
	"from",
	"fuels",
	"got",
	"government",
	"gratitude",
	"greater",
	"had",
	"handle",
	"has",
	"have",
	"he",
	"He",
	"head",
	"held",
	"highest",
	"hired",
	"holding",
	"how",
	"idea",
	"If",
	"ill",
	"ill-trained",
	"in",
	"In",
	"independent",
	"industrial",
	"inexperienced",
	"influence",
	"information",
	"informed",
	"International",
	"internet",
	"is",
	"it",
	"just",
	"keep",
	"kept",
	"killed",
	"know",
	"knows",
	"Lastly",
	"later",
	"law",
	"led",
	"legally",
	"Li",
	"Liang",
	"link",
	"little",
	"local",
	"Local",
	"Logistics",
	"made",
	"magazine",
	"main",
	"managed",
	"many",
	"matters",
	"media",
	"mentioned",
	"metres",
	"millions",
	"minimum",
	"Moreover",
	"mostly",
	"Mr",
	"narrative",
	"nature",
	"nearest",
	"neither",
	"nevertheless",
	"no",
	"nor",
	"not",
	"occasionally",
	"of",
	"off",
	"offered",
	"official",
	"officially",
	"officials",
	"on",
	"one",
	"One",
	"ones",
	"online",
	"Online",
	"only",
	"operation",
	"or",
	"out",
	"over",
	"owner",
	"owners",
	"part",
	"party",
	"pass",
	"People's",
	"permission",
	"permitted",
	"pictures",
	"platforms",
	"pointed",
	"port",
	"possible",
	"press",
	"prevention",
	"provided",
	"public",
	"public-security",
	"quantity",
	"question-and-answer",
	"questions",
	"raised",
	"ranking",
	"recorded",
	"relationships",
	"repeated",
	"required",
	"residential",
	"residents",
	"respect",
	"revealed",
	"rooms",
	"Ruihai",
	"Ruihai's",
	"rulers",
	"running",
	"safe",
	"safety",
	"said",
	"says",
	"security",
	"seemed",
	"September",
	"series",
	"session",
	"shares",
	"shown",
	"shows",
	"Shu",
	"Shushan",
	"site",
	"six",
	"so-called",
	"social",
	"Social",
	"sodium",
	"son",
	"soon",
	"sophisticated",
	"spend",
	"spread",
	"square",
	"state",
	"stored",
	"substances",
	"such",
	"Taijin",
	"televised",
	"than",
	"that",
	"the",
	"The",
	"their",
	"them—were",
	"there",
	"these",
	"they",
	"They",
	"third",
	"those",
	"Those",
	"Tianjin",
	"times",
	"to",
	"too",
	"traffic",
	"true—and",
	"two",
	"unknown",
	"until",
	"up",
	"users",
	"v",
	"warehouse",
	"was",
	"week",
	"weekend",
	"were",
	"When",
	"whereas",
	"which",
	"who",
	"workers",
	"world",
	"would",
	"young",
	"Zheng",
}
