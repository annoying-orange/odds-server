package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/rs/cors"
	"github.com/wesport/odds-server/auth"
	"github.com/wesport/odds-server/graph/generated"
	"github.com/wesport/odds-server/graph/model"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

// Resolver ...
type Resolver struct {
	redisClient   *redis.Client
	mutex         sync.Mutex
	matchChannels map[string]chan *model.Match
	subscribes    map[string]*redis.PubSub
	msgChannels   map[int]chan map[string]interface{}
	consumer      *kafka.Consumer
}

// RetryFunc ...
type RetryFunc func(int) error

// ForeverSleep ...
func ForeverSleep(d time.Duration, f RetryFunc) {
	for i := 0; ; i++ {
		err := f(i)
		if err == nil {
			return
		}
		time.Sleep(d)
	}
}

// NewResolver ...
func NewResolver(redisAddr string) (*Resolver, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "P@ssw0rd!", // no password set
		DB:       1,           // use default DB
	})

	kafakConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "wesport-subscribes",
		"auto.offset.reset": "earliest",
	}

	fmt.Printf("Kafka configs: %v\n", kafakConf)

	consumer, err := kafka.NewConsumer(kafakConf)
	if err != nil {
		panic(err)
	}

	return &Resolver{
		redisClient:   rdb,
		mutex:         sync.Mutex{},
		matchChannels: map[string]chan *model.Match{},
		subscribes:    map[string]*redis.PubSub{},
		msgChannels:   map[int]chan map[string]interface{}{},
		consumer:      consumer,
	}, nil
}

// Serve ...
func (r *Resolver) Serve(route string, port string) error {
	srv := handler.New(generated.NewExecutableSchema(generated.Config{Resolvers: r}))
	srv.AddTransport(&transport.Websocket{
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				println("Websocket CheckOrigin")
				return true
			},
		},
		InitFunc: func(ctx context.Context, payload transport.InitPayload) (context.Context, error) {
			// userId, err := validateAndGetUserID(payload["token"])
			// if err != nil {
			// 	return nil, err
			// }

			fmt.Printf("payload: %v\n", payload)

			// // get the user from the database
			// user := getUserByID(db, userId)
			user := &auth.User{
				ID: 2,
			}

			// put it in context
			userCtx := context.WithValue(ctx, auth.UserCtxKey, user)

			// and return it so the resolvers can see it
			return userCtx, nil
		},
		KeepAlivePingInterval: 10 * time.Second,
	})
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.MultipartForm{})

	srv.SetQueryCache(lru.New(1000))

	srv.Use(extension.Introspection{})
	srv.Use(extension.AutomaticPersistedQuery{
		Cache: lru.New(100),
	})

	router := chi.NewRouter()
	router.Use(auth.Middleware())
	router.Handle(route, srv)
	router.Handle("/playground", playground.Handler("GraphQL playground", route))
	router.Handle("/query", srv)

	handler := cors.AllowAll().Handler(router)

	// subscribe feeds
	go func(consumer *kafka.Consumer) {
		run := true
		err := consumer.SubscribeTopics([]string{"wesport-feeds"}, nil)
		if err != nil {
			log.Fatal(err)
			run = false
		}

		for run == true {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				var m map[string]interface{}
				if err := json.Unmarshal(e.Value, &m); err != nil {
					fmt.Println("error:", err)
				} else {
					r.mutex.Lock()
					for _, v := range r.msgChannels {
						v <- m
					}
					r.mutex.Unlock()
				}
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v\n", e)
				run = false
			}
		}

		consumer.Close()
		log.Printf("Feeds closed!\n")
	}(r.consumer)

	return http.ListenAndServe(fmt.Sprintf(":%s", port), handler)
}
