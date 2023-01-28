package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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

type MessageCondition struct {
	SportID  int
	MarketID int
	Bettypes []string
}

// Resolver ...
type Resolver struct {
	redisClient   *redis.Client
	mutex         sync.Mutex
	matchChannels map[string]chan *model.Match
	subscribes    map[string]*redis.PubSub
	msgChannels   map[int]chan map[string]interface{}
	msgConditions map[int]MessageCondition
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
func NewResolver(redisOpt *redis.FailoverOptions) (*Resolver, error) {
	rdb := redis.NewFailoverClient(redisOpt)

	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	kafakConf := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
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
		msgConditions: map[int]MessageCondition{},
		consumer:      consumer,
	}, nil
}

func (c *MessageCondition) ContainsBettype(typeID string) bool {
	for _, v := range c.Bettypes {
		if v == typeID {
			return true
		}
	}

	return false
}

func (c *MessageCondition) EqMarket(matchTime string) bool {
	if val, err := strconv.ParseInt(matchTime, 10, 64); err == nil {
		now := time.Now().UTC().Add(time.Hour * -4)
		mt := time.Unix(val, 0).UTC().Add(time.Hour * -4)

		// Live
		if c.MarketID == 1 {
			return now.After(mt)
		}

		// Today
		if c.MarketID == 2 {
			return now.After(mt) || now.Format("20060102") == mt.Format("20060102")
		}

		// Early
		if c.MarketID == 3 {
			return now.Format("20060102") > mt.Format("20060102")
		}
	}

	return false
}

// Serve ...
func (r *Resolver) Serve(route string, port string) error {
	srv := handler.New(generated.NewExecutableSchema(generated.Config{Resolvers: r}))
	srv.AddTransport(&transport.Websocket{
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		InitFunc: func(ctx context.Context, payload transport.InitPayload) (context.Context, error) {
			if token, ok := payload["Authorization"]; ok {
				user, err := auth.ValidateAndGetUser(token.(string))
				if err != nil {
					log.Printf("Authorization failed: %v\n", err)
					return nil, fmt.Errorf("Unauthorized")
				}

				log.Printf("Authorized user: %v\n", user.ID)

				// put user in context
				userCtx := context.WithValue(ctx, auth.UserCtxKey, user)

				// and return it so the resolvers can see it
				return userCtx, nil
			}

			return ctx, nil
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
	// router.Use(auth.Middleware())
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
					for userID, v := range r.msgChannels {
						if condition, ok := r.msgConditions[userID]; ok {
							typeName := m["__typename"]

							if matchTime, ok := m["matchTime"]; ok && typeName == "Match" {
								if condition.EqMarket(fmt.Sprintf("%v", matchTime)) {
									v <- m
								}
								continue
							}

							if typeID, ok := m["betTypeId"]; ok {
								if condition.ContainsBettype(fmt.Sprintf("%v", typeID)) {
									v <- m
								}
								continue
							}

							v <- m
						}
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
