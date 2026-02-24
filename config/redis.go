package config

import (
	"log"
	"context"
	"github.com/redis/go-redis/v9"
)

var Ctxx = context.Background()
var Redis *redis.Client

func InitRedis() {
	Redis = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if _, err := Redis.Ping(Ctxx).Result(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}
	log.Println("✅ Connected to Redis")
}