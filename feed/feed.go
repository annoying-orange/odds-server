package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
	"github.com/wesport/odds-server/graph/model"
)

// Feed ...
type Feed struct {
	ctx       context.Context
	rdb       *redis.Client
	in        *kafka.Consumer
	out       *kafka.Producer
	kafkaConf *kafka.ConfigMap
}

type betType struct {
	ID       int
	Name     string
	TypeName string
}

type pub struct {
	Channel string
	Message interface{}
}

var (
	leaugeFields = map[string]string{
		"leagueid":     "id",
		"sporttype":    "sportId",
		"countrycode":  "country",
		"outright":     "outright",
		"displayorder": "displayOrder",
		"leaguenameen": "en_US",
		"leaguenamecn": "zh_CHS",
		"leaguenamech": "zh_CHT",
		"leaguenameth": "th",
		"leaguenamevn": "vn",
	}

	teamFields = map[string]string{
		"sporttype":   "sportId",
		"hteamnameen": "en_US",
		"hteamnamecn": "zh_CHS",
		"hteamnamech": "zh_CHT",
		"hteamnameth": "th",
		"hteamnamevn": "vn",
	}

	matchFields = map[string]string{
		"sporttype":      "sportId",
		"leagueid":       "leagueId",
		"homeid":         "homeId",
		"hteamnameen":    "homeName",
		"livehomescore":  "homeScore",
		"homered":        "homeRedcard",
		"awayid":         "awayId",
		"ateamnameen":    "awayName",
		"liveawayscore":  "awayScore",
		"awayred":        "awayRedcard",
		"livetimer":      "liveTimer",
		"liveperiod":     "livePeriod",
		"globalshowtime": "matchTime",
		"eventstatus":    "status",
		"mc_parlay":      "allowParlay",
		"isht":           "livePeriod",
	}

	oddsFields = map[string]string{
		"matchid":    "matchId",
		"bettype":    "betTypeId",
		"hdp1":       "hdp1",
		"hdp2":       "hdp2",
		"oddsstatus": "status",
		"sort":       "displayOrder",
		"odds1a":     "oh",
		"odds2a":     "oa",
		"com1":       "o1",
		"com2":       "o2",
		"comx":       "ox",
		"cs10":       "o10",
		"cs20":       "o20",
		"cs21":       "o21",
		"cs30":       "o30",
		"cs31":       "o31",
		"cs32":       "o32",
		"cs40":       "o40",
		"cs41":       "o41",
		"cs42":       "o42",
		"cs43":       "o43",
		"cs00":       "o00",
		"cs11":       "o11",
		"cs22":       "o22",
		"cs33":       "o33",
		"cs44":       "o44",
		"cs99":       "o99",
		"cs01":       "o01",
		"cs02":       "o02",
		"cs12":       "o12",
		"cs03":       "o03",
		"cs13":       "o13",
		"cs23":       "o23",
		"cs04":       "o04",
		"cs14":       "o14",
		"cs24":       "o24",
		"cs34":       "o34",
	}

	allStatus = map[string]model.Status{
		"running":    model.StatusStable,
		"suspend":    model.StatusPause,
		"closePrice": model.StatusPause,
		"closed":     model.StatusComplete,
	}

	betTypes = map[int]*betType{
		1: {
			ID:       11002,
			Name:     "Handicap",
			TypeName: "Hdpou",
		},
		3: {
			ID:       11004,
			Name:     "Over/Under",
			TypeName: "Hdpou",
		},
		5: {
			ID:       11005,
			Name:     "1x2",
			TypeName: "MO",
		},
		7: {
			ID:       11001,
			Name:     "1H Handicap",
			TypeName: "Hdpou",
		},
		8: {
			ID:       11003,
			Name:     "1H Over/Under",
			TypeName: "Hdpou",
		},
		15: {
			ID:       11008,
			Name:     "1H 1x2",
			TypeName: "MO",
		},
		413: {
			ID:       11007,
			Name:     "Correct Score",
			TypeName: "CS",
		},
		414: {
			ID:       11014,
			Name:     "1H Correct Score",
			TypeName: "CS",
		},
		405: {
			ID:       11015,
			Name:     "2H Correct Score",
			TypeName: "CS",
		},
		2: {
			ID:       11006,
			Name:     "Odd/Even",
			TypeName: "OE",
		},
		12: {
			ID:       11016,
			Name:     "1H Odd/Even",
			TypeName: "OE",
		},
		6: {
			ID:       11009,
			Name:     "Total Goal",
			TypeName: "TG",
		},
		126: {
			ID:       11017,
			Name:     "1H Total Goal",
			TypeName: "TG",
		},
		16: {
			ID:       11011,
			Name:     "Half Time/Full Time",
			TypeName: "HTFT",
		},
		128: {
			ID:       11010,
			Name:     "Half Time/Full Time Odd/Even",
			TypeName: "HTFTOE",
		},
		14: {
			ID:       11012,
			Name:     "First Goal/Last Goal",
			TypeName: "FGLG",
		},
		127: {
			ID:       11018,
			Name:     "1H First Goal/Last Goal",
			TypeName: "FGLG",
		},
	}

	topics = []string{
		"saba-soccer-hdpou",
		"saba-soccer-1x2",
		"saba-soccer-cs",
		"saba-soccer-oe",
		"saba-soccer-tg",
		"saba-soccer-htft",
		"saba-soccer-htftoe",
		"saba-soccer-fglg",
	}

	pubs = make(chan *kafka.Message, 1024)
)

// NewFeed ...
func NewFeed(redisOpt *redis.FailoverOptions) *Feed {
	ctx := context.Background()
	rdb := redis.NewFailoverClient(redisOpt)

	ping := rdb.Ping(ctx)
	if ping.Err() != nil {
		panic(ping.Err())
	}

	log.Printf("Redis connected!\n")

	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	kafkaConf := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "wesport-feeds",
	}

	log.Printf("Kafka configs: %v\n", kafkaConf)

	return &Feed{
		ctx:       ctx,
		rdb:       rdb,
		kafkaConf: kafkaConf,
	}
}

// StartNewFeed create new feed and start it ...
func StartNewFeed(redisOpt *redis.FailoverOptions) {
	NewFeed(redisOpt).Start()
}

// Start feed
// consume from kafka and save to Redis
func (f *Feed) Start() {
	// Feed output to kafka
	go func(ctx context.Context, conf *kafka.ConfigMap) {
		p, err := kafka.NewProducer(conf)
		if err != nil {
			panic(err)
		}

		defer p.Close()

		// Delivery report handler for produced messages
		go func() {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					} else {
						// log.Printf("Delivered message to %v: %s\n", ev.TopicPartition, ev.Value)
					}
				}
			}
		}()

		for msg := range pubs {
			p.Produce(msg, nil)
		}

		p.Flush(15 * 1000)
		fmt.Println("The channel Pubs closed.")
	}(f.ctx, f.kafkaConf)

	wg := new(sync.WaitGroup)
	wg.Add(len(topics))

	// Consume all topics
	for _, topic := range topics {
		go func(topic string, conf *kafka.ConfigMap) {
			log.Printf("Consume topic: %s\n", topic)
			defer wg.Done()

			consumer, err := kafka.NewConsumer(conf)
			if err != nil {
				panic(err)
			}

			run := true
			err = consumer.SubscribeTopics([]string{topic}, nil)
			if err != nil {
				print(err)
				run = false
			}

			for run == true {
				ev := consumer.Poll(100)
				switch e := ev.(type) {
				case *kafka.Message:
					// log.Printf("Message on %s: %s\n", e.TopicPartition, string(e.Value))
					var msg map[string]interface{}
					err := json.Unmarshal(e.Value, &msg)
					if err != nil {
						log.Fatal(err)
					}

					switch msg["type"].(type) {
					case string:
						switch msg["type"].(string) {
						case "l":
							f.setLeague(msg)
						case "m":
							f.setMatch(msg)
						case "o":
							f.setOdds(msg)
						case "dm":
							f.closeMatch(msg)
						case "do":
							f.closeOdds(msg)
						}
					}
				case kafka.PartitionEOF:
					log.Printf("%% Reached %v\n", e)
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "Error: %v\n", e)
					run = false
				}
			}

			consumer.Close()
			log.Printf("Consume topic %s done.\n", topic)
		}(topic, f.kafkaConf)
	}

	wg.Wait()
	close(pubs)
}

func (f *Feed) getStatus(status string, marketID, l interface{}) model.Status {
	if val, ok := allStatus[status]; ok {
		if val == model.StatusStable && marketID != nil && l != nil {
			m := marketID.(string)
			if m == "T" && int(l.(float64)) > 0 {
				return model.StatusInProgress
			} else if m == "E" {
				return model.StatusScheduled
			}
		}

		return val
	}

	return model.StatusUnscheduled
}

func (f *Feed) getChannel(mt string) (string, error) {
	t, err := strconv.ParseInt(mt, 10, 64)
	if err != nil {
		return "", err
	}
	d := time.Unix(t, 0).UTC().Add(time.Hour * -4)
	return fmt.Sprintf("%v", d.Format("20060102")), nil
}

func (f *Feed) getTitle(mt string) (string, error) {
	t, err := strconv.ParseInt(mt, 10, 64)
	if err != nil {
		return "", err
	}

	now := time.Now().UTC().Add(time.Hour * -4)
	time := time.Unix(t, 0).UTC().Add(time.Hour * -4)

	if now.After(time) {
		return "live", nil
	} else if now.Format("20060102") == time.Format("20060102") {
		return "today", nil
	} else {
		return "early", nil
	}
}

func (f *Feed) displayHandicap(hdp float64) string {
	val := math.Abs(hdp)
	if math.Mod(val, 0.5) == 0 {
		return fmt.Sprintf("%v", val)
	}
	return fmt.Sprintf("%v-%v", val-0.25, val+0.25)
}

func (f *Feed) send(typeName string, msg interface{}) {
	if bv, err := json.Marshal(msg); err == nil {
		topic := "wesport-feeds"

		pubs <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          bv,
		}
	}
}

func (f *Feed) setLeague(msg map[string]interface{}) {
	id := strconv.FormatFloat(msg["leagueid"].(float64), 'f', -1, 64)
	key := fmt.Sprintf("league:%s", id)
	value := make(map[string]interface{})
	value["id"] = id

	for k, v := range leaugeFields {
		if o, ok := msg[k]; ok {
			value[v] = o
		}
	}

	if len(value) > 1 {
		f.rdb.HSet(f.ctx, key, value)

		// Public league
		if h := f.rdb.HGetAll(f.ctx, key); h.Err() == nil {
			val := h.Val()
			typeName := "League"
			f.send(typeName, map[string]interface{}{
				"id": val["id"],
				"name": map[string]interface{}{
					"en_US":  val["en_US"],
					"zh_CHS": val["zh_CHS"],
					"zh_CHT": val["zh_CHT"],
					"th":     val["th"],
					"vn":     val["vn"],
				},
				"sportId":      val["sportId"],
				"displayOrder": val["displayOrder"],
				"__typename":   typeName,
			})
		}
	}
}

func (f *Feed) setMatch(msg map[string]interface{}) {
	if matchID, ok := msg["matchid"]; ok {
		id := strconv.FormatFloat(matchID.(float64), 'f', -1, 64)
		typeName := "Match"

		key := fmt.Sprintf("match:%s", id)
		value := make(map[string]interface{})
		value["id"] = id
		value["__typename"] = typeName

		if showTime, ok := msg["globalshowtime"]; ok == true {
			var mt int64
			switch showTime.(type) {
			case string:
				mt, _ = strconv.ParseInt(showTime.(string), 10, 64)
			case float64:
				mt = int64(showTime.(float64))
			}

			if sportType, ok := msg["sporttype"]; ok {
				d := time.Unix(mt, 0).UTC().Add(time.Hour * -4)
				date := fmt.Sprintf("sport:%v:%v", sportType, d.Format("20060102"))
				f.rdb.ZAdd(f.ctx, date, &redis.Z{
					Score:  float64(mt),
					Member: strconv.FormatFloat(matchID.(float64), 'f', -1, 64),
				})
			}
		}

		// Home team
		if homeID, ok := msg["homeid"]; ok {
			teamID := fmt.Sprintf("%v", homeID)
			team := make(map[string]interface{})
			team["id"] = teamID

			if sportID, ok := msg["sporttype"]; ok {
				team["sportId"] = sportID
			}

			if name, ok := msg["hteamnamech"]; ok {
				team["zh_CHT"] = name
			}

			if name, ok := msg["hteamnamecn"]; ok {
				team["zh_CHS"] = name
			}

			if name, ok := msg["hteamnameen"]; ok {
				team["en_US"] = name
			}

			if name, ok := msg["hteamnameth"]; ok {
				team["th"] = name
			}

			if name, ok := msg["hteamnamevn"]; ok {
				team["vn"] = name
			}

			f.rdb.HSet(f.ctx, fmt.Sprintf("team:%s", teamID), team)
		}

		// Away team
		if awayID, ok := msg["awayid"]; ok {
			teamID := fmt.Sprintf("%v", awayID)
			team := make(map[string]interface{})
			team["id"] = teamID

			if sportID, ok := msg["sporttype"]; ok {
				team["sportId"] = sportID
			}

			if name, ok := msg["ateamnamech"]; ok {
				team["zh_CHT"] = name
			}

			if name, ok := msg["ateamnamecn"]; ok {
				team["zh_CHS"] = name
			}

			if name, ok := msg["ateamnameen"]; ok {
				team["en_US"] = name
			}

			if name, ok := msg["ateamnameth"]; ok {
				team["th"] = name
			}

			if name, ok := msg["ateamnamevn"]; ok {
				team["vn"] = name
			}

			f.rdb.HSet(f.ctx, fmt.Sprintf("team:%s", teamID), team)
		}

		for k, v := range matchFields {
			if o, ok := msg[k]; ok {
				switch k {
				case "eventstatus":
					value[v] = string(f.getStatus(fmt.Sprintf("%v", o), msg["marketid"], msg["l"]))
				case "isht":
					isht := false
					if ht, ok := msg["isht"]; ok {
						switch vv := ht.(type) {
						case float64:
							isht = vv > 0
						case bool:
							isht = vv
						}
					}

					if isht {
						// HT
						value[v] = 3
					}
				case "liveperiod":
					// Ignore liveperiod when HT
					isht := false
					if ht, ok := msg["isht"]; ok {
						switch vv := ht.(type) {
						case float64:
							isht = vv > 0
						case bool:
							isht = vv
						}
					}

					if !isht {
						value[v] = o
					}
				default:
					value[v] = o
				}
			}
		}

		if len(value) > 2 {
			// Set Match
			if h := f.rdb.HSet(f.ctx, key, value); h.Err() != nil {
				fmt.Printf("HSet %v %v: %v\n", key, value, h.Err())
			}

			// Get league ID
			if h := f.rdb.HMGet(f.ctx, key, "leagueId"); h.Err() == nil {
				val := h.Val()
				value["leagueId"] = val[0]
			}

			f.send(typeName, value)
		}
	}
}

func (f *Feed) setOdds(msg map[string]interface{}) {
	if oddsID, ok := msg["oddsid"]; ok {
		var id string
		switch oddsID.(type) {
		case string:
			id = oddsID.(string)
		case float64:
			id = strconv.FormatFloat(oddsID.(float64), 'f', -1, 64)
		}

		key := fmt.Sprintf("odds:%s", id)
		value := make(map[string]interface{})
		value["id"] = id

		for k, v := range oddsFields {
			switch k {
			case "bettype":
				if bettype, ok := msg[k]; ok {
					var v int
					switch ev := bettype.(type) {
					case string:
						v, _ = strconv.Atoi(ev)
					case float64:
						v = int(ev)
					}

					if bt, ok := betTypes[v]; ok {
						value["betTypeId"] = bt.ID
						value["betTypeName"] = bt.Name
						value["__typename"] = bt.TypeName
					}
				}
			case "oddsstatus":
				if oddsstatus, ok := msg["oddsstatus"]; ok {
					if status, ok := allStatus[fmt.Sprintf("%v", oddsstatus)]; ok {
						value[v] = string(status)
					}
				}
			default:
				if o, ok := msg[k]; ok {
					value[v] = o
				}
			}
		}

		// Make handicap
		hdp1, hdp1OK := value["hdp1"]
		hdp2, hdp2OK := value["hdp2"]

		if hdp1OK || hdp2OK {
			var btID int
			var handicap float64
			if val, ok := value["betTypeId"]; ok {
				btID = val.(int)
			} else if h := f.rdb.HGet(f.ctx, key, "betTypeId"); h.Err() == nil {
				btID, _ = strconv.Atoi(h.Val())
			}

			if hdp1OK {
				handicap = hdp1.(float64)
			}

			if hdp2OK {
				handicap -= hdp2.(float64)
			}

			value["handicap"] = handicap

			if btID == 11003 || btID == 11004 {
				// Over/Under
				value["hdp1"] = fmt.Sprintf("%v", handicap)
				value["hdp2"] = "u"
			} else if btID == 11001 || btID == 11002 {
				// Handicap
				if handicap >= 0 {
					value["hdp1"] = f.displayHandicap(handicap)
					value["hdp2"] = ""
				} else {
					value["hdp1"] = ""
					value["hdp2"] = f.displayHandicap(handicap)
				}
			}
		}

		if val, ok := msg["matchid"]; ok {
			matchID := strconv.FormatFloat(val.(float64), 'f', -1, 64)

			f.rdb.ZAdd(f.ctx, fmt.Sprintf("match:%v:odds", matchID), &redis.Z{
				Score:  msg["sort"].(float64),
				Member: id,
			})

			if typeID, ok := value["betTypeId"]; ok {
				f.rdb.ZAdd(f.ctx, fmt.Sprintf("match:%v:%v", matchID, typeID), &redis.Z{
					Score:  msg["sort"].(float64),
					Member: id,
				})
			}

			// if cmd := f.rdb.ZAdd(f.ctx, fmt.Sprintf("match:%v:odds", matchID), &redis.Z{
			// 	Score:  msg["sort"].(float64),
			// 	Member: id,
			// }); cmd.Val() > 0 {
			// 	if typeID, ok := value["betTypeId"]; ok {
			// 		f.rdb.ZIncrBy(f.ctx, fmt.Sprintf("match:%v:bettypes", matchID), 1, strconv.Itoa((typeID.(int))))
			// 	}
			// }

			// Set league ID
			if h := f.rdb.HMGet(f.ctx, fmt.Sprintf("match:%v", matchID), "matchTime", "leagueId"); h.Err() == nil {
				val := h.Val()
				if val[0] != nil && val[1] != nil {
					value["leagueId"] = val[1].(string)
				}
			}
		}

		if len(value) > 1 {
			// Set Odds
			if h := f.rdb.HSet(f.ctx, key, value); h.Err() != nil {
				fmt.Printf("HSet %v %v: %v\n", key, value, h.Err())
			}

			// Public to match channel
			if h := f.rdb.HMGet(f.ctx, key, "__typename", "matchId", "leagueId", "betTypeId"); h.Err() == nil {
				val := h.Val()
				if val[0] != nil {
					value["__typename"] = val[0]
					value["matchId"] = val[1]
					value["leagueId"] = val[2]
					value["betTypeId"] = val[3]

					f.send(val[0].(string), value)
				}
			}
		}
	}
}

func (f *Feed) closeMatch(msg map[string]interface{}) {
	if matchID, ok := msg["matchid"]; ok {
		id := strconv.FormatFloat(matchID.(float64), 'f', -1, 64)
		key := fmt.Sprintf("match:%s", id)

		if h := f.rdb.HMGet(f.ctx, key, "__typename", "id", "leagueId"); h.Err() == nil {
			val := h.Val()
			if val[0] != nil && val[1] != nil {
				value := map[string]interface{}{
					"status": string(model.StatusComplete),
				}

				if h := f.rdb.HSet(f.ctx, key, value); h.Err() != nil {
					fmt.Printf("HSet %v %v: %v", key, value, h.Err())
				}

				value["__typename"] = val[0]
				value["id"] = val[1]
				value["leagueId"] = val[2]

				f.send(val[0].(string), value)
			}
		} else {
			log.Printf("HMGet %v: %v", key, h.Err())
		}
	}
}

func (f *Feed) closeOdds(msg map[string]interface{}) {
	if oddsID, ok := msg["oddsid"]; ok {
		var id string
		switch oddsID.(type) {
		case string:
			id = oddsID.(string)
		case float64:
			id = strconv.FormatFloat(oddsID.(float64), 'f', -1, 64)
		}

		key := fmt.Sprintf("odds:%s", id)

		if h := f.rdb.HMGet(f.ctx, key, "__typename", "id", "matchId", "leagueId", "betTypeId"); h.Err() == nil {
			val := h.Val()
			if val[0] != nil && val[1] != nil {
				value := map[string]interface{}{
					"status": string(model.StatusComplete),
				}

				if h := f.rdb.HSet(f.ctx, key, value); h.Err() != nil {
					fmt.Printf("HSet %v %v: %v", key, value, h.Err())
				}

				if val[2] != nil && val[4] != nil {
					f.rdb.ZRem(f.ctx, fmt.Sprintf("match:%v:%v", val[2], val[4]), val[1])
				}

				value["__typename"] = val[0]
				value["id"] = val[1]
				value["matchId"] = val[2]
				value["leagueId"] = val[3]
				value["betTypeId"] = val[4]

				f.send(val[0].(string), value)
			}
		} else {
			log.Printf("HMGet %v: %v", key, h.Err())
		}
	}
}
