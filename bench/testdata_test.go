package bench

const (
	SmallMsg     = 1
	BigStrMsg    = 2
	BigMapBigStr = 3
	SuperBigStr  = 4
)

var requestSmallMsg = &Event{
	Timestamp: messageTs,
	User:      messageUser,
	Type:      SmallMsg,
}
var responseSmallMsg = &EventResponse{
	Timestamp: messageTs,
	Id:        messageID,
	UserID:    messageUserID,
}

var requestBigStrMsg = &Event{
	Timestamp: messageTs,
	User:      messageUser,
	Type:      BigStrMsg,
	Strdata:   message4KBStr,
}
var responseBigStrMsg = &EventResponse{
	Timestamp: messageTs,
	Id:        messageID,
	UserID:    messageUserID,
	Strdata:   message4KBStr,
}

var requestBigMapBigStrMsg = &Event{
	Timestamp: messageTs,
	User:      messageUser,
	Type:      BigMapBigStr,
	Data:      map100,
	Strdata:   message4KBStr,
}
var responseBigMapBigStrMsg = &EventResponse{
	Timestamp: messageTs,
	Id:        messageID,
	UserID:    messageUserID,
	Data:      map100,
	Strdata:   message4KBStr,
}

var requestSuperBigStrMsg = &Event{
	Timestamp: messageTs,
	User:      messageUser,
	Type:      SuperBigStr,
	Strdata:   FourMBStr,
}
var responseSuperBigStrMsg = &EventResponse{
	Timestamp: messageTs,
	Id:        messageID,
	UserID:    messageUserID,
	Strdata:   FourMBStr,
}

const (
	messageID     = "message:12345"
	messageUserID = int32(123456)
	messageTs     = "2054/12/25 21:23"
	messageUser   = "gopher-7834"

	// 4096 byte string (4 KB).
	message4KBStr = "A round hole was drilled through the thin board.The man wore a feather in his felt hat.The ink stain dried on the finished page.Use a pencil to write the first draft.A wisp of cloud hung in the blue air.Men think and plan and sometimes act.The square peg will settle in the round hole.Hurdle the pit with the aid of a long pole.Nudge gently but wake her now.The steady drip is worse than a drenching rain.The bloom of the rose lasts a few days.Those words were the cue for the actor to leave.It is late morning on the old wall clock.The best method is to fix it in place with clips.A pod is what peas always grow in.She saw a cat in the neighbor's house.She has st smart way of wearing clothes.She blushed when he gave her a white orchid.The rude laugh filled the empty room.In some form or other we need fun.The gold ring fits only a pierced ear.Pick a card and slip it under the pack.The nag pulled the frail cart along.We dress to suit the weather of most days.A brown leather bag hung from its strap.The pearl was worn in a thin silver ring.The line where the edges join was clean.Hang tinsel from both branches.Place a rosebush near the porch steps.The glow deepened in the eyes of the sweet girl.The first worm gets snapped early.A speedy man can beat this track mark.There is a strong chance it will happen once more.His wide grin earned many friends.The term ended in late June that year.The old pan was covered with hard fudge.He crawled with care along the ledge.Sever the twine with a quick snip of the knife.A force equal to that would move the earth.Sit on the perch and tell the others what to do.He sent the boy on a short errand.There is a strong chance it will happen once more.The show was a flop from the very start.The brass tube circled the high wall.Green moss grows on the northern side.Stop whistling and watch the boys march.Tuck the sheet under the edge of the mat.Even a just cause needs power to win.The team with the best timing looks good.Feed the white mouse some flower seeds.No doubt about the way the wind blows.Our plans right now are hazy.Burn peat after the logs give out.Feed the white mouse some flower seeds.We frown when events take a bad turn.The heap of fallen leaves was set on fire.His hip struck the knee of the next player.The gold vase is both rare and costly.Sell your gift to a buyer at a good gain.Smoke poured out of every crack.The lobes of her ears were pierced to hold rings.A yacht slid around the point into the bay.Bring your problems to the wise chief.The quick fox jumped on the sleeping cat.The barrel of beer was a brew of malt and hops.Help the weak to preserve their strength.Many hands help get the job done.Pink clouds floated with the breeze.The aim of the contest is to raise a great fund.He lent his coat to the tall gaunt stranger.The small pup gnawed a hole in the sock.Pitch the straw through the door of the stable.The quick fox jumped on the sleeping cat.She blushed when he gave her a white orchid.Mud was spattered on the front of his white shirt.Write at once or you may forget it.Try to trace the fine lines of the painting.Dimes showered down from all sides.The stray cat gave birth to kittens.Post no bills on this office wall.No cement will hold hard wood.Slide the catch back and open the desk.Always close the barn door tight.Ripe pears are fit for a queen's table.These coins will be needed to pay his debt.The cup cracked and spilled its contents.A pound of sugar costs more than eggs.The hat brim was wide and too droopy.The bombs left most of the town in ruins.The wagon moved on well oiled wheels.The loss of the second ship was hard to take.The straw nest housed five robins.A rag will soak up spilled water.He sent the boy on a short errand.Clothes and lodging are free to new men.Time brings us many changes.Take shelter in this tent, but keep still.Find the twin who stole the pearl necklace.He sent the figs, but kept the ripe cherries.Write a fond note to the friend you cherish. A red rabbit jumped over the brown fence, with a green turtle on it's black back in the black river water of the columbia"
)

var (
	FourMBStr = "" // to be populated by init().  About 4KB * 256 bytes, 4 MB
)

var map100 = map[string]string{
	// generated with fakedata -l 100 email | awk '{printf("\"%05d\": \"%s\",\n", NR,$0)}'
	"00001": "prrstn@test.democrat",
	"00002": "charliecwaite@example.tn",
	"00003": "RussellBishop@example.monash",
	"00004": "renbyrd@example.infiniti",
	"00005": "areandacom@example.channel",
	"00006": "arthurholcombe1@test.center",
	"00007": "lewisainslie@test.mq",
	"00008": "noxdzine@test.tvs",
	"00009": "jarjan@example.bom",
	"00010": "langate@test.xn--45q11c",
	"00011": "leemunroe@example.win",
	"00012": "therealmarvin@test.xn--zfr164b",
	"00013": "nelsonjoyce@example.select",
	"00014": "jedbridges@test.luxe",
	"00015": "brajeshwar@test.dish",
	"00016": "claudioguglieri@example.yokohama",
	"00017": "robbschiller@example.mint",
	"00018": "erwanhesry@example.xn--11b4c3d",
	"00019": "areus@test.hk",
	"00020": "ateneupopular@test.amsterdam",
	"00021": "mattbilotti@example.circle",
	"00022": "happypeter1983@test.blanco",
	"00023": "adellecharles@example.vegas",
	"00024": "ahmadajmi@test.fit",
	"00025": "therealmarvin@test.sky",
	"00026": "jonathansimmons@test.xn--3oq18vl8pn36a",
	"00027": "g3d@test.moi",
	"00028": "andrewarrow@test.lk",
	"00029": "nbirckel@test.cbn",
	"00030": "samgrover@test.kaufen",
	"00031": "alxndrustinov@test.brussels",
	"00032": "kerihenare@test.cam",
	"00033": "dc_user@test.xn--unup4y",
	"00034": "felipecsl@example.app",
	"00035": "travis_arnold@example.space",
	"00036": "ManikRathee@test.spiegel",
	"00037": "vc27@test.xn--qcka1pmc",
	"00038": "vovkasolovev@example.ls",
	"00039": "carlosgavina@test.bargains",
	"00040": "blakesimkins@example.to",
	"00041": "isacosta@example.voting",
	"00042": "tomaslau@example.ms",
	"00043": "boxmodel@test.nissay",
	"00044": "chanpory@example.mitsubishi",
	"00045": "charliecwaite@test.xn--yfro4i67o",
	"00046": "andrea211087@test.olayangroup",
	"00047": "nbirckel@test.nowtv",
	"00048": "joelcipriano@example.store",
	"00049": "catadeleon@example.kp",
	"00050": "rude@example.sc",
	"00051": "jm_denis@test.dz",
	"00052": "derienzo777@test.im",
	"00053": "9lessons@example.coffee",
	"00054": "alagoon@example.redstone",
	"00055": "andrewarrow@test.ae",
	"00056": "robbschiller@test.id",
	"00057": "increase@test.pl",
	"00058": "joelhelin@test.ups",
	"00059": "dhooyenga@example.dental",
	"00060": "therealmarvin@example.homedepot",
	"00061": "nachtmeister@test.cn",
	"00062": "orkuncaylar@example.nissay",
	"00063": "catadeleon@example.call",
	"00064": "envex@example.doctor",
	"00065": "ivanfilipovbg@test.tm",
	"00066": "jitachi@test.alsace",
	"00067": "jayrobinson@example.hamburg",
	"00068": "flashmurphy@example.cuisinella",
	"00069": "pmeissner@test.xn--pbt977c",
	"00070": "nbirckel@test.glade",
	"00071": "megdraws@example.fun",
	"00072": "commadelimited@test.church",
	"00073": "jedbridges@test.xn--9krt00a",
	"00074": "Karimmove@example.vu",
	"00075": "kinday@example.travelersinsurance",
	"00076": "randomlies@test.volvo",
	"00077": "soffes@example.mobi",
	"00078": "derienzo777@example.ua",
	"00079": "kolage@test.legal",
	"00080": "nicolai_larsen@test.quebec",
	"00081": "carlosgavina@test.fail",
	"00082": "sharvin@test.next",
	"00083": "jamiebrittain@example.dog",
	"00084": "agromov@example.adult",
	"00085": "lewisainslie@example.airtel",
	"00086": "kkusaa@example.place",
	"00087": "ahmadajmi@test.clothing",
	"00088": "d_nny_m_cher@example.amsterdam",
	"00089": "leemunroe@test.bananarepublic",
	"00090": "haligaliharun@test.wow",
	"00091": "snowshade@example.cd",
	"00092": "Stievius@example.fk",
	"00093": "kinday@test.cr",
	"00094": "dahparra@example.yachts",
	"00095": "marakasina@example.itau",
	"00096": "Chakintosh@example.tn",
	"00097": "chaensel@example.chloe",
	"00098": "baluli@test.audio",
	"00099": "dpmachado@example.wtc",
	"00100": "weglov@example.int",
}
