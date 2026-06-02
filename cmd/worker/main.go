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
	kafkainfra "golang-learning/internal/infrastructure/kafka"
	"golang-learning/internal/infrastructure/llm"
	"golang-learning/internal/infrastructure/redisstore"

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

	generator, err := llm.NewTokenGenerator(cfg.LLMProvider)
	if err != nil {
		slog.Error("llm init failed", "err", err)
		os.Exit(1)
	}

	cache := redisstore.NewConversationCache(rdb, cfg.RedisTTL)
	publisher := kafkainfra.NewEventPublisher(cfg.KafkaBrokers)
	defer publisher.Close()

	uc := usecase.NewProcessChatRequest(generator, publisher, cache)
	w := consumer.NewWorker(cfg.KafkaBrokers, uc)

	if err := w.Run(ctx); err != nil {
		slog.Error("worker error", "err", err)
		os.Exit(1)
	}
}

func parseRedisAddr(url string) string {
	if len(url) > 8 && url[:8] == "redis://" {
		return url[8:]
	}
	return url
}
