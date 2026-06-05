package main

import (
	"context"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/consumer"
	"golang-learning/internal/adapter/gateway/cache"
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
			cache.NewCallbackStore,
			func(s *cache.CallbackStoreImpl) usecase.ICallbackStore { return s },
			newStreamTokens,
			consumer.NewStreamingWorker,
		),
		fx.Invoke(runStreamingWorker),
	).Run()
}

func newStreamTokens(callbackStore usecase.ICallbackStore, cfg config.Config) *usecase.StreamTokensUseCase {
	return usecase.NewStreamTokens(callbackStore, cfg.CallbackSecret)
}

func runStreamingWorker(lc fx.Lifecycle, w *consumer.StreamingWorker, log *zap.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go func() {
				if err := w.Run(ctx); err != nil {
					log.Error("streaming worker stopped", zap.Error(err))
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
