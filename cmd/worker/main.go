package main

import (
	"context"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/consumer"
	"golang-learning/internal/adapter/gateway/event"
	"golang-learning/internal/adapter/gateway/llm"
	redisgateway "golang-learning/internal/adapter/gateway/redis"
	"golang-learning/internal/logger"
	"golang-learning/internal/usecase"

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
			event.NewEventPublisher,
			redisgateway.NewConversationCache,
			func(c *redisgateway.ConversationCache) usecase.ConversationCache { return c },
			func(p *event.EventPublisher) usecase.EventPublisher              { return p },
			usecase.NewProcessChatRequest,
			consumer.NewWorker,
		),
		fx.Invoke(runWorker),
	).Run()
}

func newRedisClient(cfg config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: parseRedisAddr(cfg.RedisURL)})
}

func newTokenGenerator(cfg config.Config) (usecase.TokenGenerator, error) {
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
