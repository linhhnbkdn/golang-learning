package main

import (
	"context"
	"net/http"

	"golang-learning/config"
	"golang-learning/internal/api"
	"golang-learning/internal/api/handler"
	"golang-learning/internal/api/middleware"
	"golang-learning/internal/api/state"
	"golang-learning/internal/application/port"
	"golang-learning/internal/application/usecase"
	kafkainfra "golang-learning/internal/infrastructure/kafka"
	"golang-learning/internal/infrastructure/postgres"
	"golang-learning/internal/infrastructure/redisstore"
	"golang-learning/internal/logger"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
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
			newSSEState,
			kafkainfra.NewEventPublisher,
			redisstore.NewConversationCache,
			redisstore.NewSessionOwnerStore,
			postgres.NewMessageStore,
			asConversationCache,
			asSessionOwnerStore,
			asMessageStore,
			asEventPublisher,
			usecase.NewSendMessage,
			usecase.NewGetHistory,
			handler.NewChatHandler,
		),
		fx.Invoke(startResponseConsumer),
		fx.Invoke(startServer),
	).Run()
}

// interface adapters — fx needs explicit wiring for concrete → interface
func asConversationCache(c *redisstore.ConversationCache) port.ConversationCache { return c }
func asSessionOwnerStore(s *redisstore.SessionOwnerStore) port.SessionOwnerStore { return s }
func asMessageStore(s *postgres.MessageStore) port.MessageStore                  { return s }
func asEventPublisher(p *kafkainfra.EventPublisher) port.EventPublisher          { return p }

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

func newSSEState() *state.SSEState { return &state.SSEState{} }

func startResponseConsumer(lc fx.Lifecycle, cfg config.Config, s *state.SSEState, log *zap.Logger) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		GroupID:  "api-sse-fan-out",
		Topic:    "chat.responses",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go api.ConsumeResponses(ctx, r, s, log)
			return nil
		},
		OnStop: func(_ context.Context) error {
			cancel()
			return r.Close()
		},
	})
}

func startServer(lc fx.Lifecycle, h *handler.ChatHandler, cfg config.Config, log *zap.Logger) {
	r := gin.Default()
	h.RegisterRoutes(r, middleware.JWT(cfg))
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			log.Info("API server starting", zap.String("port", cfg.Port))
			go srv.ListenAndServe()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}

func parseRedisAddr(url string) string {
	if len(url) > 8 && url[:8] == "redis://" {
		return url[8:]
	}
	return url
}
