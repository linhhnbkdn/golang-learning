package main

import (
	"context"

	"golang-learning/config"
	"golang-learning/internal/application/port"
	"golang-learning/internal/application/usecase"
	"golang-learning/internal/consumer"
	kafkainfra "golang-learning/internal/infrastructure/kafka"
	"golang-learning/internal/infrastructure/llm"
	"golang-learning/internal/infrastructure/redisstore"
	"golang-learning/internal/logger"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func main() {
	_ = godotenv.Load()

	fx.New(
		fx.Provide(
			config.Load,
			logger.New,
			newRedisClient,
			newTokenGenerator,
			kafkainfra.NewEventPublisher,
			redisstore.NewConversationCache,
			func(c *redisstore.ConversationCache) port.ConversationCache { return c },
			func(p *kafkainfra.EventPublisher) port.EventPublisher       { return p },
			usecase.NewProcessChatRequest,
			consumer.NewWorker,
		),
		fx.Invoke(runWorker),
	).Run()
}

func newRedisClient(cfg config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: parseRedisAddr(cfg.RedisURL)})
}

func newTokenGenerator(cfg config.Config) (port.TokenGenerator, error) {
	return llm.NewTokenGenerator(cfg.LLMProvider)
}

func runWorker(lc fx.Lifecycle, w *consumer.Worker, log *zap.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go func() {
				if err := w.Run(ctx); err != nil {
					log.Error("worker stopped", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(_ context.Context) error {
			cancel()
			return nil
		},
	})
}

func parseRedisAddr(url string) string {
	if len(url) > 8 && url[:8] == "redis://" {
		return url[8:]
	}
	return url
}
