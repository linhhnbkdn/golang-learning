package redis

import (
	"strings"

	"golang-learning/config"

	"github.com/redis/go-redis/v9"
)

func NewClient(cfg config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         parseAddr(cfg.RedisURL),
		PoolSize:     1024,
		MinIdleConns: 16,
	})
}

func parseAddr(url string) string {
	return strings.TrimPrefix(url, "redis://")
}
