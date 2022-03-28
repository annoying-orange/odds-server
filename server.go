package main

import (
	"flag"
	"log"
	"os"

	"github.com/wesport/odds-server/feed"
	"github.com/wesport/odds-server/graph"
)

const defaultPort = "9090"

var (
	redisAddr = flag.String("redis", "localhost:6379", "Redis Server hostname and port (default: localhost:6379).")
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	flag.Parse()

	// Start feed
	go feed.StartNewFeed(*redisAddr)

	// Start odds service
	s, err := graph.NewResolver(*redisAddr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("\nconnect to http://localhost:%s/playground for GraphQL playground", port)
	log.Fatal(s.Serve("/graphql", port))
}
