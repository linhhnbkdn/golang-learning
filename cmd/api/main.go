package main

import (
	"context"
	"net/http"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/http/handler"
	"golang-learning/internal/adapter/controller/http/middleware"
	wshandler "golang-learning/internal/adapter/controller/ws/handler"
	"golang-learning/internal/adapter/gateway/broker"
	"golang-learning/internal/adapter/gateway/cache"
	"golang-learning/internal/adapter/gateway/store"
	frameworkpostgres "golang-learning/internal/framework/postgres"
	frameworkredis "golang-learning/internal/framework/redis"
	"golang-learning/internal/module/logger"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
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
			frameworkpostgres.NewDB,
			broker.NewEventPublisher,
			cache.NewConversationCache,
			cache.NewSessionOwnerStore,
			cache.NewPubSubStream,
			store.NewMessageStore,
			asConversationCache,
			asSessionOwnerStore,
			asMessageStore,
			asEventPublisher,
			asPubSubStream,
			usecase.NewSendMessage,
			usecase.NewGetHistory,
			handler.NewChatHandler,
			wshandler.NewChatWsHandler,
		),
		fx.Invoke(startServer),
	).Run()
}

func asConversationCache(c *cache.ConversationCacheImpl) usecase.IConversationCache { return c }
func asSessionOwnerStore(s *cache.SessionOwnerStoreImpl) usecase.ISessionOwnerStore { return s }
func asMessageStore(s *store.MessageStoreImpl) usecase.IMessageStore                { return s }
func asEventPublisher(p *broker.EventPublisherImpl) usecase.IEventPublisher         { return p }
func asPubSubStream(s *cache.PubSubStreamImpl) usecase.IPubSubStream                { return s }

func startServer(lc fx.Lifecycle, h *handler.ChatHandler, ws *wshandler.ChatWsHandler, cfg config.Config, log *zap.Logger) {
	r := gin.Default()
	h.RegisterRoutes(r, middleware.JWT(cfg))
	ws.RegisterRoutes(r)
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
