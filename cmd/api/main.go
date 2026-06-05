package main

import (
	"context"
	"fmt"
	"net"
	"net/http"

	_ "go.uber.org/automaxprocs"

	"golang-learning/config"
	controllergrpc "golang-learning/internal/adapter/controller/grpc"
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
	pb "golang-learning/proto/gen"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
			controllergrpc.NewTokenServer,
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
	return usecase.NewSendMessage(publisher, "")
}

func startServer(lc fx.Lifecycle, h *handler.ChatHandler, stream *handler.ChatStreamHandler, ts *controllergrpc.TokenServer, cfg config.Config, log *zap.Logger) {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.AsyncLogger(log))
	authMw := middleware.JWT(cfg)
	h.RegisterRoutes(r, authMw)
	stream.RegisterRoutes(r, authMw)
	httpSrv := &http.Server{Addr: ":" + cfg.Port, Handler: r}

	grpcSrv := grpc.NewServer(
		grpc.MaxConcurrentStreams(1000),
		grpc.StreamInterceptor(controllergrpc.StreamAuthInterceptor(cfg.CallbackSecret)),
	)
	pb.RegisterTokenServiceServer(grpcSrv, ts)

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			log.Info("API server starting", zap.String("port", cfg.Port), zap.String("grpc_port", cfg.GRPCPort))
			go httpSrv.ListenAndServe()

			lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
			if err != nil {
				return err
			}
			go grpcSrv.Serve(lis)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			grpcSrv.GracefulStop()
			return httpSrv.Shutdown(ctx)
		},
	})
}
