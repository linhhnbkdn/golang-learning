package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	_ "go.uber.org/automaxprocs"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/http/handler"
	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/adapter/gateway/broker"
	"golang-learning/internal/adapter/gateway/cache"
	"golang-learning/internal/adapter/gateway/hub"
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
			store.NewMessageStore,
			asConversationCache,
			asSessionOwnerStore,
			asMessageStore,
			asEventPublisher,
			hub.New,
			asTokenHub,
			newSendMessage,
			usecase.NewGetHistory,
			handler.NewChatHandler,
			handler.NewChatStreamHandler,
			handler.NewTokenCallbackHandler,
		),
		fx.Invoke(startServer),
	).Run()
}

func asConversationCache(c *cache.ConversationCacheImpl) usecase.IConversationCache { return c }
func asSessionOwnerStore(s *cache.SessionOwnerStoreImpl) usecase.ISessionOwnerStore { return s }
func asMessageStore(s *store.MessageStoreImpl) usecase.IMessageStore                { return s }
func asEventPublisher(p *broker.EventPublisherImpl) usecase.IEventPublisher         { return p }
func asTokenHub(h *hub.TokenHub) usecase.ITokenHub                                  { return h }

func newSendMessage(publisher usecase.IEventPublisher, cfg config.Config) *usecase.SendMessageUseCase {
	hostname, _ := os.Hostname()
	callbackBase := fmt.Sprintf("http://%s:%s", hostname, cfg.Port)
	return usecase.NewSendMessage(publisher, callbackBase)
}

func startServer(lc fx.Lifecycle, h *handler.ChatHandler, stream *handler.ChatStreamHandler, cb *handler.TokenCallbackHandler, cfg config.Config, log *zap.Logger) {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.AsyncLogger(log))
	authMw := middleware.JWT(cfg)
	h.RegisterRoutes(r, authMw)
	stream.RegisterRoutes(r, authMw)
	cb.RegisterRoutes(r)
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
