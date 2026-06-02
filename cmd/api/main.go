package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang-learning/config"
	"golang-learning/internal/api"
	"golang-learning/internal/api/handler"
	"golang-learning/internal/api/state"
	"golang-learning/internal/application/usecase"
	kafkainfra "golang-learning/internal/infrastructure/kafka"
	"golang-learning/internal/infrastructure/postgres"
	"golang-learning/internal/infrastructure/redisstore"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: parseRedisAddr(cfg.RedisURL)})
	defer rdb.Close()

	// PostgreSQL
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Infrastructure
	cache := redisstore.NewConversationCache(rdb, cfg.RedisTTL)
	store := postgres.NewMessageStore(pool)
	publisher := kafkainfra.NewEventPublisher(cfg.KafkaBrokers)
	defer publisher.Close()

	// Use cases
	sendMessage := usecase.NewSendMessage(publisher)
	getHistory := usecase.NewGetHistory(cache)

	// SSE state + background consumer
	sseState := &state.SSEState{}
	api.StartResponseConsumer(ctx, cfg.KafkaBrokers, sseState)

	// HTTP server
	r := gin.Default()
	chatHandler := handler.NewChatHandler(sendMessage, getHistory, store, sseState)
	chatHandler.RegisterRoutes(r)

	slog.Info("API server starting", "port", cfg.Port)
	if err := r.Run(":" + cfg.Port); err != nil {
		slog.Error("server error", "err", err)
	}
}

func parseRedisAddr(url string) string {
	// redis://localhost:6379 → localhost:6379
	if len(url) > 8 && url[:8] == "redis://" {
		return url[8:]
	}
	return url
}
