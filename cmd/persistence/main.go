package main

import (
	"context"

	"golang-learning/config"
	"golang-learning/internal/adapter/consumer"
	"golang-learning/internal/adapter/repository/postgres"
	redisrepo "golang-learning/internal/adapter/repository/redis"
	"golang-learning/internal/logger"
	"golang-learning/internal/usecase"

	"github.com/jackc/pgx/v5/pgxpool"
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
			newPostgresPool,
			redisrepo.NewConversationCache,
			postgres.NewMessageStore,
			func(c *redisrepo.ConversationCache) usecase.ConversationCache { return c },
			func(s *postgres.MessageStore) usecase.MessageStore            { return s },
			usecase.NewPersistSession,
			consumer.NewPersistenceWorker,
		),
		fx.Invoke(runPersistence),
	).Run()
}

func newRedisClient(cfg config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: parseRedisAddr(cfg.RedisURL)})
}

func newPostgresPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error { pool.Close(); return nil },
	})
	return pool, nil
}

func runPersistence(lc fx.Lifecycle, w *consumer.PersistenceWorker, log *zap.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go func() {
				if err := w.Run(ctx); err != nil {
					log.Error("persistence worker stopped", zap.Error(err))
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
