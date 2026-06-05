package main

import (
	"context"
	"fmt"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/consumer"
	"golang-learning/internal/adapter/gateway/broker"
	"golang-learning/internal/framework/llm"
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
			newTokenGenerator,
			broker.NewEventPublisher,
			func(p *broker.EventPublisherImpl) usecase.IEventPublisher { return p },
			usecase.NewProcessChatRequest,
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
