package grid

import (
	"log"
	"time"
)

func leader(id int, topic string, quorum uint32, maxleadertime int64, in <-chan Event, out chan<- Event, exit <-chan bool) {

	ticker := time.NewTicker(TickMillis * time.Millisecond)
	defer ticker.Stop()

	state := Follower
	lasthearbeat := time.Now().Unix()

	for {
		select {
		case now := <-ticker.C:
			if now.Unix()-lasthearbeat > HeartTimeout {
				state = Follower
			}
		case m := <-in:
			var cmdmsg *CmdMesg

			// Extract command message.
			switch msg := m.Message().(type) {
			case *CmdMesg:
				cmdmsg = msg
			default:
				continue
			}

			// Check for type of message.
			switch data := cmdmsg.Data.(type) {
			case Ping:
				lasthearbeat = time.Now().Unix()
				log.Printf("%v %v", data, state)
			}
		}
	}

}
