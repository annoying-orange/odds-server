package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/wesport/odds-server/auth"
	"github.com/wesport/odds-server/graph/generated"
	"github.com/wesport/odds-server/graph/model"
)

func (r *mutationResolver) IncreaseMarket(ctx context.Context, id string, increases map[string]interface{}) (map[string]interface{}, error) {
	h := r.redisClient.HMGet(ctx, fmt.Sprintf("odds:%v", id), "matchId", "__typename")
	if h.Err() != nil {
		return nil, h.Err()
	}

	val := h.Val()
	if val[0] == nil || val[1] == nil {
		return nil, fmt.Errorf("Invalid market [%s]", id)
	}

	key := fmt.Sprintf("odds:%v:increases", id)
	resp := map[string]interface{}{
		"id":         id,
		"matchId":    val[0],
		"__typename": val[1],
	}

	for field, val := range increases {
		fmt.Printf("HIncr %v %v %v\n", key, field, val)
		inrc, _ := val.(json.Number).Float64()
		re := r.redisClient.HIncrByFloat(ctx, key, field, inrc)
		if re.Err() != nil {
			fmt.Printf("%v\n", re.Err())
			return nil, re.Err()
		}
		resp[field] = re.Val()
	}

	return resp, nil
}

func (r *mutationResolver) UpdateMatch(ctx context.Context, id string, fields map[string]interface{}) (map[string]interface{}, error) {
	key := fmt.Sprintf("match:%v", id)
	h := r.redisClient.HMGet(ctx, key, "__typename", "leagueId")
	if h.Err() != nil {
		return nil, fmt.Errorf("Invalid match [%s]", id)
	}

	val := h.Val()
	resp := map[string]interface{}{
		"id":         id,
		"__typename": val[0],
		"leagueId":   val[1],
	}

	values := make(map[string]interface{})

	if status, ok := fields["status"]; ok {
		values["status"] = status
		values["unpublish"] = status == string(model.StatusUnscheduled)
	}

	if len(values) > 0 {
		re := r.redisClient.HSet(ctx, key, values)
		if re.Err() != nil {
			fmt.Printf("Error HSET %v %v: %v\n", key, fields, re.Err())
			return nil, re.Err()
		}

		for field, val := range fields {
			resp[field] = val
		}
	}

	return resp, nil
}

func (r *queryResolver) Leagues(ctx context.Context, date string) ([]*model.League, error) {
	leagues := []*model.League{}
	seam := make(map[string]int)
	key := fmt.Sprintf("sport:1:%v", strings.ReplaceAll(date, "-", ""))
	ids := r.redisClient.ZRange(ctx, key, 0, -1)

	if ids.Err() != nil {
		fmt.Printf("ZRANGE %v 0 -1: %v\n", key, ids.Err())
	}

	for _, id := range ids.Val() {
		if m, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("match:%v", id)).Result(); err == nil {
			matchID := m["id"]
			matchTime, _ := strconv.Atoi(m["matchTime"])
			sportID, _ := strconv.Atoi(m["sportId"])
			leagueID := m["leagueId"]
			matchStatus := model.Status(m["status"])
			livePeriod, _ := strconv.Atoi(m["livePeriod"])
			liveTimer, _ := strconv.Atoi(m["liveTimer"])
			homeID, _ := strconv.Atoi(m["homeId"])
			homeScore, _ := strconv.Atoi(m["homeScore"])
			homeRedcard, _ := strconv.Atoi(m["homeRedcard"])
			awayID, _ := strconv.Atoi(m["awayId"])
			awayScore, _ := strconv.Atoi(m["awayScore"])
			awayRedcard, _ := strconv.Atoi(m["awayRedcard"])

			if isClosed(matchStatus) {
				continue
			}

			markets := []model.Market{}
			// Market
			olist := r.redisClient.ZRange(ctx, fmt.Sprintf("match:%v:odds", matchID), 0, -1)
			for _, oid := range olist.Val() {
				if o, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("odds:%v", oid)).Result(); err == nil {
					if status, ok := o["status"]; ok && status != string(model.StatusComplete) {
						id := o["id"]
						do, _ := strconv.Atoi(o["displayOrder"])
						btID, _ := strconv.Atoi(o["betTypeId"])
						incr, _ := r.redisClient.HGetAll(ctx, fmt.Sprintf("odds:%v:increases", oid)).Result()

						// Handicap & Over/Undeer
						if btID == 11002 || btID == 11004 || btID == 11001 || btID == 11003 {
							handicap, _ := strconv.ParseFloat(o["handicap"], 64)
							o1, _ := strconv.ParseFloat(o["o1"], 64)
							o2, _ := strconv.ParseFloat(o["o2"], 64)
							incr1, _ := strconv.ParseFloat(incr["o1"], 64)
							incr2, _ := strconv.ParseFloat(incr["o2"], 64)

							var hdp1, hdp2 string
							// Over/Undeer
							if btID == 11004 || btID == 11003 {
								hdp1 = o["handicap"]
								hdp2 = "u"
							} else {
								if handicap >= 0 {
									hdp1 = displayHandicap(handicap)
									hdp2 = ""
								} else {
									hdp1 = ""
									hdp2 = displayHandicap(handicap)
								}
							}

							markets = append(markets, &model.Hdpou{
								ID:           id,
								MatchID:      matchID,
								Status:       model.Status(status),
								DisplayOrder: do,
								BetTypeID:    btID,
								Handicap:     handicap,
								Hdp1:         hdp1,
								Hdp2:         hdp2,
								O1:           o1,
								O2:           o2,
								I1:           incr1,
								I2:           incr2,
							})
						}

						// 1x2
						if btID == 11005 || btID == 11008 {
							o1, _ := strconv.ParseFloat(o["o1"], 64)
							o2, _ := strconv.ParseFloat(o["o2"], 64)
							ox, _ := strconv.ParseFloat(o["ox"], 64)
							incr1, _ := strconv.ParseFloat(incr["o1"], 64)
							incr2, _ := strconv.ParseFloat(incr["o2"], 64)
							incrx, _ := strconv.ParseFloat(incr["ox"], 64)

							markets = append(markets, &model.Mo{
								ID:           id,
								MatchID:      matchID,
								Status:       model.Status(status),
								BetTypeID:    btID,
								O1:           o1,
								O2:           o2,
								Ox:           ox,
								I1:           incr1,
								I2:           incr2,
								Ix:           incrx,
								DisplayOrder: do,
							})
						}
					}
				}
			}

			// Match
			match := &model.Match{
				ID:           matchID,
				MatchTime:    matchTime,
				SportID:      sportID,
				LeagueID:     leagueID,
				Status:       matchStatus,
				LivePeriod:   livePeriod,
				LiveTimer:    liveTimer,
				HomeID:       homeID,
				HomeName:     m["homeName"],
				HomeScore:    homeScore,
				HomeRedcard:  homeRedcard,
				AwayID:       awayID,
				AwayName:     m["awayName"],
				AwayScore:    awayScore,
				AwayRedcard:  awayRedcard,
				More:         0,
				DisplayOrder: 1,
				Markets:      markets,
			}

			if i, ok := seam[leagueID]; ok {
				leagues[i].Matches = append(leagues[i].Matches, match)
			} else {
				// League
				if m, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("league:%v", leagueID)).Result(); err == nil {
					do, _ := strconv.Atoi(m["displayOrder"])
					league := &model.League{
						ID:           m["id"],
						Name:         m["en_US"],
						DisplayOrder: do,
						Matches:      []*model.Match{match},
					}
					leagues = append(leagues, league)

					seam[m["id"]] = len(leagues) - 1
				}
			}
		} else {
			fmt.Printf("HGETALL %v: %v\n", fmt.Sprintf("match:%v", id), err)
		}
	}

	// Sort by display order asc, name asc
	sort.SliceStable(leagues, func(i, j int) bool {
		if leagues[i].DisplayOrder != leagues[j].DisplayOrder {
			return leagues[i].DisplayOrder < leagues[j].DisplayOrder
		}
		return strings.Compare(leagues[i].Name, leagues[j].Name) == -1
	})

	return leagues, nil
}

func (r *queryResolver) Matches(ctx context.Context, date string) ([]model.MatchResult, error) {
	user := auth.ForContext(ctx)
	fmt.Printf("%v: %v\n", date, user)

	result := []model.MatchResult{}
	key := fmt.Sprintf("date:%v:matches", date)
	ids := r.redisClient.ZRange(ctx, key, 0, -1)
	exists := make(map[string]bool)

	for _, id := range ids.Val() {
		if m, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("match:%v", id)).Result(); err == nil {
			matchID := m["id"]
			matchTime, _ := strconv.Atoi(m["matchTime"])
			sportID, _ := strconv.Atoi(m["sportId"])
			leagueID := m["leagueId"]
			livePeriod, _ := strconv.Atoi(m["livePeriod"])
			liveTimer, _ := strconv.Atoi(m["liveTimer"])
			homeID, _ := strconv.Atoi(m["homeId"])
			homeScore, _ := strconv.Atoi(m["homeScore"])
			homeRedcard, _ := strconv.Atoi(m["homeRedcard"])
			awayID, _ := strconv.Atoi(m["awayId"])
			awayScore, _ := strconv.Atoi(m["awayScore"])
			awayRedcard, _ := strconv.Atoi(m["awayRedcard"])

			// League
			if l, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("league:%v", leagueID)).Result(); err == nil {
				if _, ok := exists[l["id"]]; !ok {
					displayOrder, _ := strconv.Atoi(l["displayOrder"])
					result = append(result, &model.League{
						ID:           l["id"],
						Name:         l["en_US"],
						DisplayOrder: displayOrder,
					})

					exists[l["id"]] = true
				}
			}

			// Match
			result = append(result, &model.Match{
				ID:           matchID,
				MatchTime:    matchTime,
				SportID:      sportID,
				LeagueID:     leagueID,
				Status:       model.StatusScheduled,
				LivePeriod:   livePeriod,
				LiveTimer:    liveTimer,
				HomeID:       homeID,
				HomeName:     m["homeName"],
				HomeScore:    homeScore,
				HomeRedcard:  homeRedcard,
				AwayID:       awayID,
				AwayName:     m["awayName"],
				AwayScore:    awayScore,
				AwayRedcard:  awayRedcard,
				More:         0,
				DisplayOrder: 1,
			})

			// Odds
			olist := r.redisClient.ZRange(ctx, fmt.Sprintf("match:%v:odds", matchID), 0, -1)
			for _, oid := range olist.Val() {
				if o, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("odds:%v", oid)).Result(); err == nil {
					// status, _ := strconv.Atoi(o["status"])
					displayOrder, _ := strconv.Atoi(o["displayOrder"])
					btID, _ := strconv.Atoi(o["betTypeId"])
					incr, err := r.redisClient.HGetAll(ctx, fmt.Sprintf("odds:%v:increases", oid)).Result()
					if err != nil {
						print(err)
					}

					// Handicap & Over/Undeer
					if btID == 11002 || btID == 11004 || btID == 11001 || btID == 11003 {
						handicap, _ := strconv.ParseFloat(o["handicap"], 64)
						o1, _ := strconv.ParseFloat(o["o1"], 64)
						o2, _ := strconv.ParseFloat(o["o2"], 64)
						incr1, _ := strconv.ParseFloat(incr["o1"], 64)
						incr2, _ := strconv.ParseFloat(incr["o2"], 64)

						var hdp1, hdp2 string
						// Over/Undeer
						if btID == 11004 || btID == 11003 {
							hdp1 = o["handicap"]
							hdp2 = "u"
						} else {
							if handicap >= 0 {
								hdp1 = displayHandicap(handicap)
								hdp2 = ""
							} else {
								hdp1 = ""
								hdp2 = displayHandicap(handicap)
							}
						}

						result = append(result, &model.Hdpou{
							ID:           o["id"],
							MatchID:      matchID,
							Status:       model.StatusStable,
							DisplayOrder: displayOrder,
							BetTypeID:    btID,
							Handicap:     handicap,
							Hdp1:         hdp1,
							Hdp2:         hdp2,
							O1:           o1,
							O2:           o2,
							I1:           incr1,
							I2:           incr2,
						})
					}

					// 1x2
					if btID == 11005 || btID == 11008 {
						o1, _ := strconv.ParseFloat(o["o1"], 64)
						o2, _ := strconv.ParseFloat(o["o2"], 64)
						ox, _ := strconv.ParseFloat(o["ox"], 64)
						incr1, _ := strconv.ParseFloat(incr["o1"], 64)
						incr2, _ := strconv.ParseFloat(incr["o2"], 64)
						incrx, _ := strconv.ParseFloat(incr["ox"], 64)

						result = append(result, &model.Mo{
							ID:           o["id"],
							MatchID:      matchID,
							Status:       model.StatusStable,
							DisplayOrder: displayOrder,
							BetTypeID:    btID,
							O1:           o1,
							O2:           o2,
							Ox:           ox,
							I1:           incr1,
							I2:           incr2,
							Ix:           incrx,
						})
					}
				}
			}
		}
	}

	return result, nil
}

func (r *subscriptionResolver) Subscribe(ctx context.Context, channels []string) (<-chan map[string]interface{}, error) {
	user := auth.ForContext(ctx)
	log.Printf("Subscribe: %v, %v", user, channels)

	// Create new channel for request
	msgChannel := make(chan map[string]interface{}, 1)
	r.mutex.Lock()
	r.msgChannels[user.ID] = msgChannel
	r.mutex.Unlock()

	log.Printf("%v: %v\n", user.ID, r.msgChannels[user.ID])

	// Delete channel when done
	go func() {
		<-ctx.Done()
		r.mutex.Lock()
		if mc, ok := r.msgChannels[user.ID]; ok {
			close(mc)
			delete(r.msgChannels, user.ID)
		}
		r.mutex.Unlock()
		fmt.Printf("Done: %v\n", user.ID)
	}()

	return msgChannel, nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

// Subscription returns generated.SubscriptionResolver implementation.
func (r *Resolver) Subscription() generated.SubscriptionResolver { return &subscriptionResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
type subscriptionResolver struct{ *Resolver }

func displayHandicap(hdp float64) string {
	val := math.Abs(hdp)
	if math.Mod(val, 0.5) == 0 {
		return fmt.Sprintf("%v", val)
	}
	return fmt.Sprintf("%v-%v", val-0.25, val+0.25)
}

func isClosed(status model.Status) bool {
	return status == model.StatusComplete || status == model.StatusEnd || status == model.StatusRefund
}

func appendLeague(slice []model.League, leauge model.League) []model.League {
	ok := true
	for _, e := range slice {
		if e.ID == leauge.ID {
			ok = false
			break
		}
	}

	if ok {
		return append(slice, leauge)
	}

	return slice
}
