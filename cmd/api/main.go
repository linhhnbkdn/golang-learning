package main

import (
	"context"
	"net/http"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/consumer"
	"golang-learning/internal/adapter/controller/http/handler"
	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/adapter/controller/http/state"
	"golang-learning/internal/adapter/gateway/event"
	"golang-learning/internal/adapter/gateway/postgres"
	redisgateway "golang-learning/internal/adapter/gateway/redis"
	frameworkpostgres "golang-learning/internal/framework/postgres"
	frameworkredis "golang-learning/internal/framework/redis"
	"golang-learning/internal/module/logger"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
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
			frameworkredis.NewClient,
			frameworkpostgres.NewDB,
			newSSEState,
			event.NewEventPublisher,
			redisgateway.NewConversationCache,
			redisgateway.NewSessionOwnerStore,
			redisgateway.NewRequestOwnerStore,
			postgres.NewMessageStore,
			asConversationCache,
			asSessionOwnerStore,
			asRequestOwnerStore,
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

func asConversationCache(c *redisgateway.ConversationCache) usecase.ConversationCache { return c }
func asSessionOwnerStore(s *redisgateway.SessionOwnerStore) usecase.SessionOwnerStore { return s }
func asRequestOwnerStore(r *redisgateway.RequestOwnerStore) usecase.RequestOwnerStore { return r }
func asMessageStore(s *postgres.MessageStore) usecase.MessageStore                    { return s }
func asEventPublisher(p *event.EventPublisher) usecase.EventPublisher                 { return p }

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
			go consumer.ConsumeResponses(ctx, r, s, log)
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
