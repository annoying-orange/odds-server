package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/wesport/odds-server/feed"
	"github.com/wesport/odds-server/graph"
)

const defaultPort = "9090"

func getRedisOpt() *redis.FailoverOptions {
	sentinelAddrs := os.Getenv("REDIS_SENTINELS")
	if sentinelAddrs == "" {
		sentinelAddrs = "localhost:26379,localhost:26380,localhost:26381"
	}

	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = "P@ssw0rd!"
	}

	db, err := strconv.Atoi(os.Getenv("REDIS_DATABASE"))
	if err != nil {
		db = 1
	}

	return &redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: strings.Split(sentinelAddrs, ","),
		Password:      password,
		DB:            db,
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	redisOpt := getRedisOpt()

	// Start feed
	go feed.StartNewFeed(redisOpt)

	// Start odds service
	s, err := graph.NewResolver(redisOpt)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("\nconnect to http://localhost:%s/playground for GraphQL playground", port)
	log.Fatal(s.Serve("/", port))
}
