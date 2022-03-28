package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
	"github.com/wesport/odds-server/graph/model"
)

// Feed ...
type Feed struct {
	RedisAddr string
	ctx       context.Context
	in        *kafka.Consumer
	out       *kafka.Producer
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
		"odds1a":     "o1",
		"odds2a":     "o2",
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
		"closed":     model.StatusComplete,
		"closePrice": model.StatusComplete,
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
)

// NewFeed ...
func NewFeed(redisAddr string) *Feed {
	return &Feed{
		RedisAddr: redisAddr,
		ctx:       context.Background(),
	}
}

// StartNewFeed create new feed and start it ...
func StartNewFeed(redisAddr string) {
	NewFeed(redisAddr).Start()
}

// Start feed
// consume from kafka and save to Redis
func (f *Feed) Start() {
	ctx := f.ctx

	// flag.String("redis", "localhost:6379", "Redis hostname")

	// pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	// pflag.Parse()
	// viper.BindPFlags(pflag.CommandLine)

	// host := viper.GetString("redis")

	// rdb := redis.NewFailoverClient(&redis.FailoverOptions{
	// 	MasterName:    "master",
	// 	SentinelAddrs: []string{"localhost:26379", "localhost:26380", "localhost:26381"},
	// })

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "P@ssw0rd!", // no password set
		DB:       1,           // use default DB
	})

	ping := rdb.Ping(ctx)
	if ping.Err() != nil {
		panic(ping.Err())
	}

	fmt.Printf("Redis connected!\n")

	// Read configs from redis
	configs := rdb.HGetAll(ctx, "configs:odds.server")
	if configs.Err() != nil {
		fmt.Printf("Fetch configs error: %v\n", configs.Err())
	}

	kafakConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "wesport-feeds",
		"auto.offset.reset": "earliest",
	}

	fmt.Printf("Kafka configs: %v\n", kafakConf)

	consumer, err := kafka.NewConsumer(kafakConf)
	if err != nil {
		panic(err)
	}

	run := true
	err = consumer.SubscribeTopics([]string{"saba-soccer-hdpou"}, nil)
	if err != nil {
		print(err)
		run = false
	}

	pubs := make(chan *kafka.Message, 1024)

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
	}(ctx, kafakConf)

	for run == true {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			// fmt.Printf("Message on %s: %s\n", e.TopicPartition, string(e.Value))
			var objmap map[string]interface{}
			err := json.Unmarshal(e.Value, &objmap)
			if err != nil {
				log.Fatal(err)
			}

			switch objmap["type"].(type) {
			case string:
				switch objmap["type"].(string) {
				case "l":
					id := strconv.FormatFloat(objmap["leagueid"].(float64), 'f', -1, 64)
					key := fmt.Sprintf("league:%s", id)
					values := make(map[string]interface{})
					values["id"] = id

					for k, v := range leaugeFields {
						if o, ok := objmap[k]; ok {
							values[v] = o
						}
					}

					if len(values) > 1 {
						rdb.HSet(ctx, key, values)
					}
				case "m":
					if matchID, ok := objmap["matchid"]; ok == true {
						id := strconv.FormatFloat(matchID.(float64), 'f', -1, 64)
						key := fmt.Sprintf("match:%s", id)
						values := make(map[string]interface{})
						values["id"] = id
						values["__typename"] = "Match"

						if showTime, ok := objmap["globalshowtime"]; ok == true {
							var mt int64
							switch showTime.(type) {
							case string:
								mt, _ = strconv.ParseInt(showTime.(string), 10, 64)
							case float64:
								mt = int64(showTime.(float64))
							}

							d := time.Unix(mt, 0).UTC().Add(time.Hour * -4)
							date := fmt.Sprintf("sport:%v:%v", objmap["sporttype"], d.Format("20060102"))
							rdb.ZAdd(ctx, date, &redis.Z{
								Score:  float64(mt),
								Member: strconv.FormatFloat(matchID.(float64), 'f', -1, 64),
							})

							// Set channel
							if channel, err := f.getChannel(strconv.FormatInt(mt, 10)); err == nil {
								values["channel"] = channel
							}
						}

						// Home Team
						if homeID, ok := objmap["homeid"]; ok {
							teamID := strconv.FormatFloat(homeID.(float64), 'f', -1, 64)
							team := make(map[string]interface{})
							team["id"] = teamID

							if sportID, ok := objmap["sporttype"]; ok {
								team["sportId"] = sportID
							}

							if name, ok := objmap["hteamnamech"]; ok {
								team["zh_CHT"] = name
							}

							if name, ok := objmap["hteamnamecn"]; ok {
								team["zh_CHS"] = name
							}

							if name, ok := objmap["hteamnameen"]; ok {
								team["en_US"] = name
							}

							if name, ok := objmap["hteamnameth"]; ok {
								team["th"] = name
							}

							if name, ok := objmap["hteamnamevn"]; ok {
								team["vn"] = name
							}

							rdb.HSet(ctx, fmt.Sprintf("team:%s", teamID), team)
						}

						// Away Team
						if awayID, ok := objmap["awayid"]; ok {
							teamID := strconv.FormatFloat(awayID.(float64), 'f', -1, 64)
							team := make(map[string]interface{})
							team["id"] = teamID

							if sportID, ok := objmap["sporttype"]; ok {
								team["sportId"] = sportID
							}

							if name, ok := objmap["ateamnamech"]; ok {
								team["zh_CHT"] = name
							}

							if name, ok := objmap["ateamnamecn"]; ok {
								team["zh_CHS"] = name
							}

							if name, ok := objmap["ateamnameen"]; ok {
								team["en_US"] = name
							}

							if name, ok := objmap["ateamnameth"]; ok {
								team["th"] = name
							}

							if name, ok := objmap["ateamnamevn"]; ok {
								team["vn"] = name
							}

							rdb.HSet(ctx, fmt.Sprintf("team:%s", teamID), team)
						}

						for k, v := range matchFields {
							if o, ok := objmap[k]; ok {
								switch k {
								case "eventstatus":
									values[v] = string(f.getStatus(o.(string), objmap["marketid"], objmap["l"]))
								case "isht":
									if ht, ok := objmap["isht"]; ok && ht.(bool) {
										// HT
										values[v] = 3
									}
								case "liveperiod":
									// Ignore liveperiod when HT
									if ht, ok := objmap["isht"]; !ok || !ht.(bool) {
										values[v] = o
									}
								default:
									values[v] = o
								}
							}
						}

						if len(values) > 2 {
							// Set Match
							if h := rdb.HSet(ctx, key, values); h.Err() != nil {
								fmt.Printf("HSet %v %v: %v\n", key, values, h.Err())
							}

							// Public
							var channel interface{}
							h := rdb.HMGet(ctx, key, "channel", "leagueId")
							if h.Err() == nil {
								val := h.Val()
								channel = val[0]
								values["leagueId"] = val[1]
							}

							if channel != nil {
								// Public League
								if leagueID, ok := objmap["leagueid"]; ok {
									leagueKey := fmt.Sprintf("league:%s", strconv.FormatFloat(leagueID.(float64), 'f', -1, 64))
									if h := rdb.HGetAll(ctx, leagueKey); h.Err() == nil {
										val := h.Val()
										f.send(pubs, map[string]interface{}{
											"id":           val["id"],
											"name":         val["en_US"],
											"sportId":      val["sportId"],
											"displayOrder": val["displayOrder"],
											"__typename":   "League",
										})
									} else {
										fmt.Printf("HGETALL %v: %v", leagueKey, h.Err())
									}
								}

								f.send(pubs, values)
							}
						}
					}
				case "o":
					if oddsID, ok := objmap["oddsid"]; ok {
						var id string
						switch oddsID.(type) {
						case string:
							id = oddsID.(string)
						case float64:
							id = strconv.FormatFloat(oddsID.(float64), 'f', -1, 64)
						}
						key := fmt.Sprintf("odds:%s", id)
						values := make(map[string]interface{})
						values["id"] = id

						if val, ok := objmap["matchid"]; ok == true {
							mid := strconv.FormatFloat(val.(float64), 'f', -1, 64)

							rdb.ZAdd(ctx, fmt.Sprintf("match:%v:odds", mid), &redis.Z{
								Score:  objmap["sort"].(float64),
								Member: id,
							})

							// Set channel
							h := rdb.HMGet(ctx, fmt.Sprintf("match:%v", mid), "matchTime", "leagueId")
							if h.Err() == nil {
								val := h.Val()
								if val[0] != nil && val[1] != nil {
									if channel, err := f.getChannel(val[0].(string)); err == nil {
										values["channel"] = channel
									}
									values["leagueId"] = val[1].(string)
								}
							}
						}

						for k, v := range oddsFields {
							switch k {
							case "bettype":
								if bettype, ok := objmap[k]; ok {
									if bt, ok := betTypes[int(bettype.(float64))]; ok {
										values["betTypeId"] = bt.ID
										values["betTypeName"] = bt.Name
										values["__typename"] = bt.TypeName
									}
								}
							case "oddsstatus":
								if oddsstatus, ok := objmap["oddsstatus"]; ok {
									if status, ok := allStatus[oddsstatus.(string)]; ok {
										values[v] = string(status)
									}
								}
							default:
								if o, ok := objmap[k]; ok {
									values[v] = o
								}
							}
						}

						// Make handicap
						hdp1, hdp1OK := values["hdp1"]
						hdp2, hdp2OK := values["hdp2"]

						if hdp1OK || hdp2OK {
							var btID int
							var handicap float64
							if val, ok := values["betTypeId"]; ok {
								btID = val.(int)
							} else if h := rdb.HGet(ctx, key, "betTypeId"); h.Err() == nil {
								btID, _ = strconv.Atoi(h.Val())
							}

							if hdp1OK {
								handicap = hdp1.(float64)
							}

							if hdp2OK {
								handicap -= hdp2.(float64)
							}

							values["handicap"] = handicap

							if btID == 11003 || btID == 11004 {
								// Over/Under
								values["hdp1"] = fmt.Sprintf("%v", handicap)
								values["hdp2"] = "u"
							} else if btID == 11001 || btID == 11002 {
								// Handicap
								if handicap >= 0 {
									values["hdp1"] = f.displayHandicap(handicap)
									values["hdp2"] = ""
								} else {
									values["hdp1"] = ""
									values["hdp2"] = f.displayHandicap(handicap)
								}
							}
						}

						if len(values) > 1 {
							// Set Odds
							if h := rdb.HSet(ctx, key, values); h.Err() != nil {
								fmt.Printf("HSet %v %v: %v\n", key, values, h.Err())
							}

							// Public to match channel
							h := rdb.HMGet(ctx, key, "__typename", "channel", "matchId", "leagueId")
							if h.Err() == nil {
								val := h.Val()
								if val[1] != nil {
									values["__typename"] = val[0]
									values["matchId"] = val[2]
									values["leagueId"] = val[3]

									f.send(pubs, values)
								}
							}
						}
					}
				case "dm":
					if matchID, ok := objmap["matchid"]; ok {
						id := strconv.FormatFloat(matchID.(float64), 'f', -1, 64)
						key := fmt.Sprintf("match:%s", id)

						if h := rdb.HMGet(ctx, key, "__typename", "id", "leagueId", "channel"); h.Err() == nil {
							val := h.Val()
							values := map[string]interface{}{
								"status": string(model.StatusComplete),
							}

							if r := rdb.HSet(ctx, key, values); r.Err() != nil {
								fmt.Printf("HSet %v %v: %v", key, values, r.Err())
							}

							if val[3] != nil {
								values["__typename"] = val[0]
								values["id"] = val[1]
								values["leagueId"] = val[2]

								f.send(pubs, values)
							}
						} else {
							fmt.Printf("HMGet %v: %v", key, h.Err())
						}
					}
				case "do":
					if oddsID, ok := objmap["oddsid"]; ok {
						var id string
						switch oddsID.(type) {
						case string:
							id = oddsID.(string)
						case float64:
							id = strconv.FormatFloat(oddsID.(float64), 'f', -1, 64)
						}
						key := fmt.Sprintf("odds:%s", id)

						if h := rdb.HMGet(ctx, key, "__typename", "id", "matchId", "leagueId", "channel"); h.Err() == nil {
							val := h.Val()
							values := map[string]interface{}{
								"status": string(model.StatusComplete),
							}

							if do := rdb.HSet(ctx, key, values); do.Err() != nil {
								fmt.Printf("HSet %v %v: %v", key, values, do.Err())
							}

							if val[4] != nil {
								values["__typename"] = val[0]
								values["id"] = val[1]
								values["matchId"] = val[2]
								values["leagueId"] = val[3]

								f.send(pubs, values)
							}
						} else {
							fmt.Printf("HMGet %v: %v", key, h.Err())
						}
					}
				}
			}
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "Error: %v\n", e)
			run = false
		}
	}

	close(pubs)
	consumer.Close()
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

func (f *Feed) displayHandicap(hdp float64) string {
	val := math.Abs(hdp)
	if math.Mod(val, 0.5) == 0 {
		return fmt.Sprintf("%v", val)
	}
	return fmt.Sprintf("%v-%v", val-0.25, val+0.25)
}

func (f *Feed) send(pubs chan<- *kafka.Message, msg interface{}) {
	if bv, err := json.Marshal(msg); err == nil {
		topic := "wesport-feeds"

		pubs <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          bv,
		}
	}
}
