package redis_test

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Commands", func() {
	values := make(map[string]interface{})
	values["id"] = 41542529
	values["__typename"] = "Match"
	values["allowParlay"] = 21
	values["awayId"] = 674756
	values["awayName"] = "RB Leipzig (V)"
	values["awayRedcard"] = 0
	values["awayScore"] = 0
	values["homeId"] = 674928
	values["homeName"] = "Atalanta (V)"
	values["homeRedcard"] = 0
	values["homeScore"] = 0
	values["leagueId"] = 95730
	values["livePeriod"] = 0
	values["liveTimer"] = 1612773840
	values["matchTime"] = 1612773900
	values["sportId"] = 1
	values["status"] = "STABLE"

	redisOptions := &redis.Options{
		Addr: ":6379",
		DB:   15,

		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRetries: -1,

		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        time.Minute,
		IdleCheckFrequency: 100 * time.Millisecond,
	}

	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions)
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("matches", func() {
		Context("With more than 300 pages", func() {
			It("should HSet", func() {
				ok, err := client.HSet(ctx, "match:41542529", values).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).To(Equal(int64(17)))

				keys, err := client.HKeys(ctx, "hash").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(keys).To(ConsistOf([]string{
					"id",
					"awayRedcard",
					"homeName",
					"leagueId",
					"__typename",
					"awayName",
					"homeId",
					"liveTimer",
					"allowParlay",
					"awayId",
					"awayScore",
					"homeRedcard",
					"homeScore",
					"matchTime",
					"livePeriod",
					"sportId",
					"status",
				}))
			})
		})
	})
})
