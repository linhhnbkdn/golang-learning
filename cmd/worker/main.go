package main

import (
	"context"
	"fmt"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/consumer"
	"golang-learning/internal/adapter/gateway/broker"
	"golang-learning/internal/adapter/gateway/cache"
	"golang-learning/internal/framework/llm"
	frameworkredis "golang-learning/internal/framework/redis"
	"golang-learning/internal/module/logger"
	"golang-learning/internal/usecase"

	"github.com/joho/godotenv"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func main() {
	_ = godotenv.Load()

	fx.New(
		fx.Provide(
			config.Load,
			logger.New,
			frameworkredis.NewClient,
			newTokenGenerator,
			broker.NewEventPublisher,
			cache.NewConversationCache,
			func(c *cache.ConversationCacheImpl) usecase.IConversationCache { return c },
			func(p *broker.EventPublisherImpl) usecase.IEventPublisher      { return p },
			newProcessChatRequest,
			consumer.NewWorker,
		),
		fx.Invoke(runWorker),
	).Run()
}

func newTokenGenerator(cfg config.Config) (usecase.ITokenGenerator, error) {
	switch cfg.LLMProvider {
	case "mock", "":
		return &llm.MockLLMStrategy{}, nil
	default:
		return nil, fmt.Errorf("unknown LLM provider: %s", cfg.LLMProvider)
	}
}

func newProcessChatRequest(
	generator usecase.ITokenGenerator,
	publisher usecase.IEventPublisher,
	cache usecase.IConversationCache,
	cfg config.Config,
) *usecase.ProcessChatRequestUseCase {
	return usecase.NewProcessChatRequest(generator, publisher, cache, cfg.APICallbackBase, cfg.CallbackSecret)
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
