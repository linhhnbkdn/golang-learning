package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang-learning/config"
	"golang-learning/internal/application/usecase"
	"golang-learning/internal/consumer"
	"golang-learning/internal/infrastructure/postgres"
	"golang-learning/internal/infrastructure/redisstore"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: parseRedisAddr(cfg.RedisURL)})
	defer rdb.Close()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	cache := redisstore.NewConversationCache(rdb, cfg.RedisTTL)
	store := postgres.NewMessageStore(pool)

	uc := usecase.NewPersistSession(cache, store)
	w := consumer.NewPersistenceWorker(cfg.KafkaBrokers, uc)

	if err := w.Run(ctx); err != nil {
		slog.Error("persistence worker error", "err", err)
		os.Exit(1)
	}
}

func parseRedisAddr(url string) string {
	if len(url) > 8 && url[:8] == "redis://" {
		return url[8:]
	}
	return url
}
