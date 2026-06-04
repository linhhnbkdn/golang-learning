# WebSocket + Redis Pub/Sub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace 2-round-trip SSE (POST /chat + GET /stream/:id) with a single WebSocket connection backed by Redis Pub/Sub (zero memory accumulation).

**Architecture:** Client connects WS `/ws/chat/:session_id?token=JWT`, sends `{"content":"..."}`, receives token stream, WS closes on DONE. Worker publishes tokens to Redis Pub/Sub channel `pubsub:session:{id}` — all WS connections on the same session_id receive every token (fan-out).

**Tech Stack:** Go 1.26, Gin, gorilla/websocket, go-redis/v9 Pub/Sub, Kafka (kafka-go), uber/fx DI.

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| ADD    | `internal/adapter/controller/ws/handler/chat.go` | WS handler — auth, ownership, read/write loops |
| ADD    | `internal/adapter/gateway/cache/pubsub_stream.go` | Redis Pub/Sub implementation of IPubSubStream |
| MODIFY | `internal/usecase/boundary.go` | Swap ISSEStream→IPubSubStream, remove IRequestOwnerStore |
| MODIFY | `internal/usecase/process_chat_request.go` | Use IPubSubStream instead of ISSEStream |
| MODIFY | `internal/adapter/controller/http/middleware/jwt.go` | Extract ParseJWT helper for WS reuse |
| MODIFY | `internal/adapter/controller/http/handler/chat.go` | Remove PostChat, StreamResponse, requestOwner |
| DELETE | `internal/adapter/gateway/cache/sse_stream.go` | Replaced by pubsub_stream.go |
| DELETE | `internal/adapter/gateway/cache/request_owner.go` | No longer needed |
| MODIFY | `cmd/worker/main.go` | Wire IPubSubStream instead of ISSEStream |
| MODIFY | `cmd/api/main.go` | Wire WS handler, remove SSE/RequestOwner wiring |

---

## Task 1: Add gorilla/websocket dependency

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add dependency**

```bash
cd /home/li/Desktop/golang-learning
go get github.com/gorilla/websocket
```

Expected: `go.mod` now contains `github.com/gorilla/websocket`.

- [ ] **Step 2: Verify build still passes**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add gorilla/websocket dependency"
```

---

## Task 2: Update boundary.go — swap interfaces

**Files:**
- Modify: `internal/usecase/boundary.go`

- [ ] **Step 1: Replace the file content**

```go
package usecase

import (
	"context"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type IConversationCache interface {
	SaveMessage(ctx context.Context, msg entity.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error)
}

type IEventPublisher interface {
	PublishRequest(ctx context.Context, req shared.ChatRequest) error
	PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error
}

type PubSubToken struct {
	RequestID string
	Delta     string
	Done      bool
}

type IPubSubStream interface {
	Publish(ctx context.Context, sessionID, requestID, delta string, done bool) error
	Subscribe(ctx context.Context, sessionID string) (<-chan PubSubToken, func(), error)
}

type IMessageStore interface {
	SaveMessage(ctx context.Context, msg entity.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error)
}

type ITokenGenerator interface {
	Generate(ctx context.Context, content string) (<-chan string, error)
}

type ISessionOwnerStore interface {
	ClaimOwner(ctx context.Context, sessionID, userID string) (bool, error)
	GetOwner(ctx context.Context, sessionID string) (string, error)
}

// Output ports — use cases call these to deliver results to the presenter.

type ISendMessageOutputPort interface {
	PresentRequestID(requestID string)
	PresentError(err error)
}

type IGetHistoryOutputPort interface {
	PresentMessages(messages []entity.Message)
	PresentError(err error)
}
```

- [ ] **Step 2: Verify it compiles (will fail on dependents — that's expected)**

```bash
go build ./internal/usecase/...
```

Expected: boundary.go compiles; other packages that reference ISSEStream/IRequestOwnerStore will fail — fix in later tasks.

---

## Task 3: Extract ParseJWT helper

**Files:**
- Modify: `internal/adapter/controller/http/middleware/jwt.go`

- [ ] **Step 1: Add ParseJWT function and refactor JWT middleware to use it**

Replace the entire file:

```go
package middleware

import (
	"net/http"
	"strings"

	"golang-learning/config"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const UserIDKey = "user_id"

// ParseJWT validates a raw JWT string and returns the user_id claim.
func ParseJWT(raw, secret string) (string, error) {
	token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return "", jwt.ErrSignatureInvalid
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", jwt.ErrSignatureInvalid
	}
	userID, ok := claims[UserIDKey].(string)
	if !ok || userID == "" {
		return "", jwt.ErrSignatureInvalid
	}
	return userID, nil
}

// JWT validates Authorization: Bearer <token> and injects user_id into context.
func JWT(cfg config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}
		userID, err := ParseJWT(strings.TrimPrefix(header, "Bearer "), cfg.JWTSecret)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		c.Set(UserIDKey, userID)
		c.Next()
	}
}
```

- [ ] **Step 2: Verify**

```bash
go build ./internal/adapter/controller/http/middleware/...
```

Expected: compiles cleanly.

- [ ] **Step 3: Commit**

```bash
git add internal/adapter/controller/http/middleware/jwt.go
git commit -m "refactor: extract ParseJWT helper for WS reuse"
```

---

## Task 4: Create pubsub_stream.go

**Files:**
- Create: `internal/adapter/gateway/cache/pubsub_stream.go`

- [ ] **Step 1: Write the implementation**

```go
package cache

import (
	"context"
	"encoding/json"
	"fmt"

	"golang-learning/internal/usecase"

	"github.com/redis/go-redis/v9"
)

type PubSubStreamImpl struct {
	client *redis.Client
}

func NewPubSubStream(client *redis.Client) *PubSubStreamImpl {
	return &PubSubStreamImpl{client: client}
}

func (s *PubSubStreamImpl) key(sessionID string) string {
	return fmt.Sprintf("pubsub:session:%s", sessionID)
}

func (s *PubSubStreamImpl) Publish(ctx context.Context, sessionID, requestID, delta string, done bool) error {
	doneVal := "0"
	if done {
		doneVal = "1"
	}
	payload, err := json.Marshal(map[string]string{
		"request_id": requestID,
		"delta":      delta,
		"done":       doneVal,
	})
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, s.key(sessionID), payload).Err()
}

func (s *PubSubStreamImpl) Subscribe(ctx context.Context, sessionID string) (<-chan usecase.PubSubToken, func(), error) {
	ps := s.client.Subscribe(ctx, s.key(sessionID))
	if _, err := ps.Receive(ctx); err != nil {
		ps.Close()
		return nil, nil, err
	}

	ch := make(chan usecase.PubSubToken, 100)
	go func() {
		defer close(ch)
		msgs := ps.Channel()
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var data map[string]string
				if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
					continue
				}
				token := usecase.PubSubToken{
					RequestID: data["request_id"],
					Delta:     data["delta"],
					Done:      data["done"] == "1",
				}
				ch <- token
				if token.Done {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	unsubscribe := func() { ps.Close() }
	return ch, unsubscribe, nil
}
```

- [ ] **Step 2: Verify**

```bash
go build ./internal/adapter/gateway/cache/...
```

Expected: compiles cleanly.

---

## Task 5: Update process_chat_request.go

**Files:**
- Modify: `internal/usecase/process_chat_request.go`

- [ ] **Step 1: Swap ISSEStream → IPubSubStream**

Replace the entire file:

```go
package usecase

import (
	"context"
	"strings"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type ProcessChatRequestUseCase struct {
	generator ITokenGenerator
	publisher IEventPublisher
	cache     IConversationCache
	pubSub    IPubSubStream
}

func NewProcessChatRequest(
	generator ITokenGenerator,
	publisher IEventPublisher,
	cache IConversationCache,
	pubSub IPubSubStream,
) *ProcessChatRequestUseCase {
	return &ProcessChatRequestUseCase{
		generator: generator,
		publisher: publisher,
		cache:     cache,
		pubSub:    pubSub,
	}
}

func (uc *ProcessChatRequestUseCase) Execute(ctx context.Context, req shared.ChatRequest) error {
	fullResponse, err := uc.streamTokens(ctx, req)
	if err != nil {
		return err
	}

	if err := uc.cacheMessages(ctx, req, fullResponse); err != nil {
		return err
	}

	return uc.publisher.PublishCompleted(ctx, shared.ChatCompleted{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
	})
}

func (uc *ProcessChatRequestUseCase) streamTokens(ctx context.Context, req shared.ChatRequest) (string, error) {
	tokenCh, err := uc.generator.Generate(ctx, req.Content)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	for token := range tokenCh {
		sb.WriteString(token)
		if err := uc.pubSub.Publish(ctx, req.SessionID, req.RequestID, token, false); err != nil {
			return "", err
		}
	}

	return sb.String(), uc.pubSub.Publish(ctx, req.SessionID, req.RequestID, "", true)
}

func (uc *ProcessChatRequestUseCase) cacheMessages(ctx context.Context, req shared.ChatRequest, fullResponse string) error {
	if err := uc.cache.SaveMessage(ctx, entity.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      entity.RoleUser,
		Content:   req.Content,
	}); err != nil {
		return err
	}
	return uc.cache.SaveMessage(ctx, entity.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      entity.RoleAssistant,
		Content:   fullResponse,
	})
}
```

- [ ] **Step 2: Verify**

```bash
go build ./internal/usecase/...
```

Expected: compiles cleanly.

---

## Task 6: Create WS handler

**Files:**
- Create: `internal/adapter/controller/ws/handler/chat.go`

- [ ] **Step 1: Write the WS handler**

```go
package wshandler

import (
	"net/http"
	"sync/atomic"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type ChatWsHandler struct {
	sendMessage *usecase.SendMessageUseCase
	ownerStore  usecase.ISessionOwnerStore
	pubSub      usecase.IPubSubStream
	cfg         config.Config
	log         *zap.Logger
	upgrader    websocket.Upgrader
}

func NewChatWsHandler(
	sendMessage *usecase.SendMessageUseCase,
	ownerStore usecase.ISessionOwnerStore,
	pubSub usecase.IPubSubStream,
	cfg config.Config,
	log *zap.Logger,
) *ChatWsHandler {
	return &ChatWsHandler{
		sendMessage: sendMessage,
		ownerStore:  ownerStore,
		pubSub:      pubSub,
		cfg:         cfg,
		log:         log,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *ChatWsHandler) RegisterRoutes(r *gin.Engine) {
	r.GET("/ws/chat/:session_id", h.Handle)
}

type clientMsg struct {
	Content string `json:"content"`
}

type wsPresenter struct {
	RequestID string
	Err       error
}

func (p *wsPresenter) PresentRequestID(id string) { p.RequestID = id }
func (p *wsPresenter) PresentError(err error)      { p.Err = err }

func (h *ChatWsHandler) Handle(c *gin.Context) {
	sessionID := c.Param("session_id")

	userID, err := middleware.ParseJWT(c.Query("token"), h.cfg.JWTSecret)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.log.Error("ws upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	ctx := c.Request.Context()

	owned, err := h.ownerStore.ClaimOwner(ctx, sessionID, userID)
	if err != nil || !owned {
		conn.WriteJSON(gin.H{"error": "forbidden"})
		return
	}

	tokenCh, unsubscribe, err := h.pubSub.Subscribe(ctx, sessionID)
	if err != nil {
		h.log.Error("pubsub subscribe failed", zap.Error(err))
		conn.WriteJSON(gin.H{"error": "internal error"})
		return
	}
	defer unsubscribe()

	// writeCh serialises all WS writes through one goroutine (gorilla: one concurrent writer).
	writeCh := make(chan any, 16)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for msg := range writeCh {
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		}
	}()

	// Forward Pub/Sub tokens to WS client.
	var streaming int32
	go func() {
		for token := range tokenCh {
			writeCh <- map[string]any{
				"request_id": token.RequestID,
				"delta":      token.Delta,
				"done":       token.Done,
			}
			if token.Done {
				atomic.StoreInt32(&streaming, 0)
				conn.Close()
				return
			}
		}
	}()

	// Read loop — one message at a time from client.
	for {
		var msg clientMsg
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}

		if atomic.LoadInt32(&streaming) == 1 {
			writeCh <- gin.H{"error": "stream in progress"}
			continue
		}

		atomic.StoreInt32(&streaming, 1)
		p := &wsPresenter{}
		h.sendMessage.Execute(ctx, sessionID, msg.Content, p)
		if p.Err != nil {
			h.log.Error("send message failed", zap.Error(p.Err))
			writeCh <- gin.H{"error": p.Err.Error()}
			atomic.StoreInt32(&streaming, 0)
		}
	}

	close(writeCh)
	<-done
}
```

- [ ] **Step 2: Verify**

```bash
go build ./internal/adapter/controller/ws/...
```

Expected: compiles cleanly.

---

## Task 7: Update HTTP chat handler

**Files:**
- Modify: `internal/adapter/controller/http/handler/chat.go`

- [ ] **Step 1: Remove PostChat, StreamResponse, sseStream, requestOwner**

Replace the entire file:

```go
package handler

import (
	"errors"
	"net/http"

	"golang-learning/internal/adapter/controller/http/middleware"
	httppresenter "golang-learning/internal/adapter/presenter/http"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type ChatHandler struct {
	getHistory *usecase.GetHistoryUseCase
	store      usecase.IMessageStore
	ownerStore usecase.ISessionOwnerStore
	log        *zap.Logger
}

func NewChatHandler(
	getHistory *usecase.GetHistoryUseCase,
	store usecase.IMessageStore,
	ownerStore usecase.ISessionOwnerStore,
	log *zap.Logger,
) *ChatHandler {
	return &ChatHandler{
		getHistory: getHistory,
		store:      store,
		ownerStore: ownerStore,
		log:        log,
	}
}

func (h *ChatHandler) RegisterRoutes(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	auth := r.Group("/", authMiddleware)
	auth.GET("/history/:session_id", h.GetHistory)
	auth.GET("/history/:session_id/db", h.GetHistoryDB)
}

func (h *ChatHandler) GetHistory(c *gin.Context) {
	sessionID := c.Param("session_id")
	if h.guardSession(c, sessionID) {
		return
	}
	p := &httppresenter.GetHistoryPresenter{}
	h.getHistory.Execute(c.Request.Context(), sessionID, p)
	if p.Err != nil {
		h.log.Error("get history failed", zap.Error(p.Err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": p.Err.Error()})
		return
	}
	c.JSON(http.StatusOK, p.Messages)
}

func (h *ChatHandler) GetHistoryDB(c *gin.Context) {
	sessionID := c.Param("session_id")
	if h.guardSession(c, sessionID) {
		return
	}
	messages, err := h.store.GetHistory(c.Request.Context(), sessionID)
	if err != nil {
		h.log.Error("get history db failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	p := &httppresenter.GetHistoryPresenter{}
	p.PresentMessages(messages)
	c.JSON(http.StatusOK, p.Messages)
}

func (h *ChatHandler) guardSession(c *gin.Context, sessionID string) bool {
	userID := c.GetString(middleware.UserIDKey)
	owner, err := h.ownerStore.GetOwner(c.Request.Context(), sessionID)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return true
		}
		h.log.Error("get session owner failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return true
	}
	if owner != userID {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return true
	}
	return false
}
```

- [ ] **Step 2: Verify**

```bash
go build ./internal/adapter/controller/http/...
```

Expected: compiles cleanly.

---

## Task 8: Delete sse_stream.go and request_owner.go

**Files:**
- Delete: `internal/adapter/gateway/cache/sse_stream.go`
- Delete: `internal/adapter/gateway/cache/request_owner.go`

- [ ] **Step 1: Delete the files**

```bash
rm internal/adapter/gateway/cache/sse_stream.go
rm internal/adapter/gateway/cache/request_owner.go
```

- [ ] **Step 2: Verify**

```bash
go build ./internal/adapter/gateway/cache/...
```

Expected: compiles cleanly.

---

## Task 9: Update cmd/worker/main.go

**Files:**
- Modify: `cmd/worker/main.go`

- [ ] **Step 1: Replace ISSEStream with IPubSubStream**

Replace the entire file:

```go
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
			cache.NewPubSubStream,
			func(c *cache.ConversationCacheImpl) usecase.IConversationCache { return c },
			func(p *broker.EventPublisherImpl) usecase.IEventPublisher      { return p },
			func(s *cache.PubSubStreamImpl) usecase.IPubSubStream           { return s },
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
```

- [ ] **Step 2: Verify**

```bash
go build ./cmd/worker/...
```

Expected: compiles cleanly.

---

## Task 10: Update cmd/api/main.go

**Files:**
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Wire WS handler, remove SSE/RequestOwner**

Replace the entire file:

```go
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
```

- [ ] **Step 2: Verify full build**

```bash
go build ./...
```

Expected: all packages compile cleanly with zero errors.

- [ ] **Step 3: Commit everything**

```bash
git add -A
git commit -m "feat: replace SSE with WebSocket + Redis Pub/Sub"
```

---

## Task 11: E2E Test

- [ ] **Step 1: Install wscat**

```bash
npm install -g wscat
```

- [ ] **Step 2: Generate a JWT token**

```bash
TOKEN=$(go run ./cmd/gentoken/main.go)
echo $TOKEN
```

- [ ] **Step 3: Start the stack**

```bash
docker compose -f docker-compose.prod.yml up --build -d
```

Wait ~15s for Kafka to be ready.

- [ ] **Step 4: Connect WebSocket and send a message**

```bash
wscat -c "ws://localhost:8000/ws/chat/test-session-1?token=$TOKEN"
```

Once connected, type and press Enter:
```json
{"content": "hello world"}
```

Expected output (tokens stream in, then done):
```
< {"request_id":"...","delta":"hello","done":false}
< {"request_id":"...","delta":" world","done":false}
< {"request_id":"...","delta":"","done":true}
```

Connection closes automatically after `done: true`.

- [ ] **Step 5: Test fan-out (2 tabs)**

Open terminal A:
```bash
wscat -c "ws://localhost:8000/ws/chat/test-session-2?token=$TOKEN"
```

Open terminal B (same session):
```bash
wscat -c "ws://localhost:8000/ws/chat/test-session-2?token=$TOKEN"
```

Send from terminal A:
```json
{"content": "hello from tab 1"}
```

Expected: both terminal A and B receive the same token stream.

- [ ] **Step 6: Test block on concurrent send**

In terminal A (before stream finishes), try sending from terminal B:
```json
{"content": "second message"}
```

Expected: `{"error":"stream in progress"}`

- [ ] **Step 7: Commit test evidence**

```bash
git commit --allow-empty -m "test: e2e WebSocket + Pub/Sub verified"
```

---

## Task 12: Deploy to production

- [ ] **Step 1: Rebuild and redeploy**

```bash
docker compose -f docker-compose.prod.yml up --build -d --force-recreate api worker
```

- [ ] **Step 2: Check logs**

```bash
docker compose -f docker-compose.prod.yml logs -f api worker
```

Expected: no errors, both services up.

- [ ] **Step 3: Verify Redis has zero stream keys (old sse: keys gone)**

```bash
docker compose -f docker-compose.prod.yml exec redis redis-cli keys "sse:*"
```

Expected: `(empty array)`

- [ ] **Step 4: Commit docker-compose if changed**

```bash
git add docker-compose.prod.yml
git diff --staged --quiet || git commit -m "chore: deploy WS + Pub/Sub to prod"
```
