# Streamable HTTP Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the WebSocket transport layer with chunked HTTP streaming — `POST /chat/:session_id` streams NDJSON tokens back to the client; Kafka and Redis Pub/Sub are untouched.

**Architecture:** The Client↔API protocol changes from WebSocket to HTTP chunked transfer. The API subscribes to Redis Pub/Sub before publishing to Kafka, then flushes each token as a newline-delimited JSON chunk. All internal plumbing (Kafka, Redis Pub/Sub, Worker) remains identical.

**Tech Stack:** Gin (`c.Writer.Flush()`), `encoding/json`, existing `middleware.JWT`, existing `usecase.IPubSubStream` / `ISessionOwnerStore` / `SendMessageUseCase`.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| CREATE | `internal/adapter/controller/http/handler/chat_stream.go` | HTTP streaming handler — owns the POST /chat/:session_id route |
| CREATE | `internal/adapter/controller/http/handler/chat_stream_test.go` | Unit tests for the streaming handler |
| MODIFY | `cmd/api/main.go` | Remove WS wiring, add ChatStreamHandler |
| DELETE | `internal/adapter/controller/ws/` | Entire directory — no longer needed |
| MODIFY | `Makefile` | Replace wscat targets with curl |
| RUN | `go mod tidy` | Drop gorilla/websocket |

---

## Task 1: Write failing test for ChatStreamHandler

**Files:**
- Create: `internal/adapter/controller/http/handler/chat_stream_test.go`

- [ ] **Step 1: Create the test file**

```go
package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang-learning/internal/adapter/controller/http/handler"
	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ── mocks ────────────────────────────────────────────────────────────────────

type mockOwnerStore struct{ owned bool }

func (m *mockOwnerStore) ClaimOwner(_ context.Context, _, _ string) (bool, error) {
	return m.owned, nil
}
func (m *mockOwnerStore) GetOwner(_ context.Context, _ string) (string, error) {
	return "user1", nil
}

type mockPubSub struct{ tokens []usecase.PubSubToken }

func (m *mockPubSub) Publish(_ context.Context, _, _, _ string, _ bool) error { return nil }
func (m *mockPubSub) Subscribe(_ context.Context, _ string) (<-chan usecase.PubSubToken, func(), error) {
	ch := make(chan usecase.PubSubToken, len(m.tokens))
	for _, t := range m.tokens {
		ch <- t
	}
	close(ch)
	return ch, func() {}, nil
}

type mockEventPublisher struct{}

func (m *mockEventPublisher) PublishRequest(_ context.Context, _ shared.ChatRequest) error {
	return nil
}
func (m *mockEventPublisher) PublishCompleted(_ context.Context, _ shared.ChatCompleted) error {
	return nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func newTestRouter(tokens []usecase.PubSubToken, owned bool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	pubSub := &mockPubSub{tokens: tokens}
	ownerStore := &mockOwnerStore{owned: owned}
	sendMessage := usecase.NewSendMessage(&mockEventPublisher{})
	log, _ := zap.NewDevelopment()

	h := handler.NewChatStreamHandler(sendMessage, ownerStore, pubSub, log)

	auth := r.Group("/", func(c *gin.Context) {
		c.Set(middleware.UserIDKey, "user1")
		c.Next()
	})
	auth.POST("/chat/:session_id", h.Handle)
	return r
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestChatStreamHandler_StreamsTokens(t *testing.T) {
	tokens := []usecase.PubSubToken{
		{RequestID: "req1", Delta: "Hello", Done: false},
		{RequestID: "req1", Delta: " world", Done: false},
		{RequestID: "req1", Delta: "", Done: true},
	}
	r := newTestRouter(tokens, true)

	body := `{"content":"hi"}`
	req := httptest.NewRequest(http.MethodPost, "/chat/session-1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	type chunk struct {
		RequestID string `json:"request_id"`
		Delta     string `json:"delta"`
		Done      bool   `json:"done"`
	}

	var chunks []chunk
	scanner := bufio.NewScanner(bytes.NewReader(w.Body.Bytes()))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var c chunk
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatalf("failed to decode chunk: %v — line: %q", err, line)
		}
		chunks = append(chunks, c)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0].Delta != "Hello" {
		t.Errorf("chunk[0].Delta = %q, want %q", chunks[0].Delta, "Hello")
	}
	if chunks[1].Delta != " world" {
		t.Errorf("chunk[1].Delta = %q, want %q", chunks[1].Delta, " world")
	}
	if !chunks[2].Done {
		t.Error("last chunk should have done=true")
	}
}

func TestChatStreamHandler_ForbiddenWhenNotOwner(t *testing.T) {
	r := newTestRouter(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/chat/session-1", strings.NewReader(`{"content":"hi"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestChatStreamHandler_BadRequestOnEmptyContent(t *testing.T) {
	r := newTestRouter(nil, true)

	req := httptest.NewRequest(http.MethodPost, "/chat/session-1", strings.NewReader(`{"content":""}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
```

- [ ] **Step 2: Run tests — expect compile error (handler not yet defined)**

```bash
cd /home/li/Desktop/golang-learning
go test ./internal/adapter/controller/http/handler/...
```

Expected: compile error — `handler.NewChatStreamHandler undefined`

---

## Task 2: Implement ChatStreamHandler

**Files:**
- Create: `internal/adapter/controller/http/handler/chat_stream.go`

- [ ] **Step 1: Create the handler**

```go
package handler

import (
	"encoding/json"
	"net/http"

	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type ChatStreamHandler struct {
	sendMessage *usecase.SendMessageUseCase
	ownerStore  usecase.ISessionOwnerStore
	pubSub      usecase.IPubSubStream
	log         *zap.Logger
}

func NewChatStreamHandler(
	sendMessage *usecase.SendMessageUseCase,
	ownerStore usecase.ISessionOwnerStore,
	pubSub usecase.IPubSubStream,
	log *zap.Logger,
) *ChatStreamHandler {
	return &ChatStreamHandler{
		sendMessage: sendMessage,
		ownerStore:  ownerStore,
		pubSub:      pubSub,
		log:         log,
	}
}

func (h *ChatStreamHandler) RegisterRoutes(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	auth := r.Group("/", authMiddleware)
	auth.POST("/chat/:session_id", h.Handle)
}

type streamBody struct {
	Content string `json:"content"`
}

type streamPresenter struct {
	Err error
}

func (p *streamPresenter) PresentRequestID(_ string) {}
func (p *streamPresenter) PresentError(err error)    { p.Err = err }

func (h *ChatStreamHandler) Handle(c *gin.Context) {
	sessionID := c.Param("session_id")
	userID := c.GetString(middleware.UserIDKey)

	var body streamBody
	if err := c.ShouldBindJSON(&body); err != nil || body.Content == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "content required"})
		return
	}

	ctx := c.Request.Context()

	owned, err := h.ownerStore.ClaimOwner(ctx, sessionID, userID)
	if err != nil || !owned {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	tokenCh, unsubscribe, err := h.pubSub.Subscribe(ctx, sessionID)
	if err != nil {
		h.log.Error("pubsub subscribe failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer unsubscribe()

	p := &streamPresenter{}
	h.sendMessage.Execute(ctx, sessionID, body.Content, p)
	if p.Err != nil {
		h.log.Error("send message failed", zap.Error(p.Err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": p.Err.Error()})
		return
	}

	c.Header("Content-Type", "application/x-ndjson")
	c.Header("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(c.Writer)
	for token := range tokenCh {
		if err := enc.Encode(map[string]any{
			"request_id": token.RequestID,
			"delta":      token.Delta,
			"done":       token.Done,
		}); err != nil {
			h.log.Error("stream write failed", zap.Error(err))
			return
		}
		c.Writer.Flush()
		if token.Done {
			return
		}
	}
}
```

- [ ] **Step 2: Run tests — expect PASS**

```bash
cd /home/li/Desktop/golang-learning
go test ./internal/adapter/controller/http/handler/... -v
```

Expected:
```
--- PASS: TestChatStreamHandler_StreamsTokens
--- PASS: TestChatStreamHandler_ForbiddenWhenNotOwner
--- PASS: TestChatStreamHandler_BadRequestOnEmptyContent
PASS
```

- [ ] **Step 3: Commit**

```bash
git add internal/adapter/controller/http/handler/chat_stream.go \
        internal/adapter/controller/http/handler/chat_stream_test.go
git commit -m "feat: add HTTP streaming handler for POST /chat/:session_id"
```

---

## Task 3: Update DI wiring in cmd/api/main.go

**Files:**
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Replace WS handler with ChatStreamHandler**

Replace the entire file content:

```go
package main

import (
	"context"
	"net/http"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/http/handler"
	"golang-learning/internal/adapter/controller/http/middleware"
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
			handler.NewChatStreamHandler,
		),
		fx.Invoke(startServer),
	).Run()
}

func asConversationCache(c *cache.ConversationCacheImpl) usecase.IConversationCache { return c }
func asSessionOwnerStore(s *cache.SessionOwnerStoreImpl) usecase.ISessionOwnerStore { return s }
func asMessageStore(s *store.MessageStoreImpl) usecase.IMessageStore                { return s }
func asEventPublisher(p *broker.EventPublisherImpl) usecase.IEventPublisher         { return p }
func asPubSubStream(s *cache.PubSubStreamImpl) usecase.IPubSubStream                { return s }

func startServer(lc fx.Lifecycle, h *handler.ChatHandler, stream *handler.ChatStreamHandler, cfg config.Config, log *zap.Logger) {
	r := gin.Default()
	authMw := middleware.JWT(cfg)
	h.RegisterRoutes(r, authMw)
	stream.RegisterRoutes(r, authMw)
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

- [ ] **Step 2: Verify build compiles**

```bash
cd /home/li/Desktop/golang-learning
go build ./cmd/api/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/api/main.go
git commit -m "feat: wire ChatStreamHandler into API, remove WS handler from DI"
```

---

## Task 4: Delete the ws/ directory

**Files:**
- Delete: `internal/adapter/controller/ws/`

- [ ] **Step 1: Remove the directory**

```bash
rm -rf /home/li/Desktop/golang-learning/internal/adapter/controller/ws
```

- [ ] **Step 2: Verify full build still passes**

```bash
cd /home/li/Desktop/golang-learning
go build ./...
go test ./...
```

Expected: all green, no references to `ws/handler` remain.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: remove ws/ handler directory — replaced by HTTP streaming"
```

---

## Task 5: Update Makefile targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Replace `chat` target**

Find and replace the `chat:` block:

```makefile
chat:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@curl -N -s -X POST http://localhost:$(PORT)/chat/$(SESSION) \
		-H "Authorization: Bearer $(T)" \
		-H "Content-Type: application/json" \
		-d "{\"content\":\"$(MSG)\"}"
```

- [ ] **Step 2: Replace `prod-chat` target**

```makefile
prod-chat:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@curl -N -s -X POST http://localhost:$(PORT)/chat/$(SESSION) \
		-H "Authorization: Bearer $(T)" \
		-H "Content-Type: application/json" \
		-d "{\"content\":\"$(MSG)\"}"
```

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: update chat/prod-chat Makefile targets to use curl HTTP streaming"
```

---

## Task 6: Remove gorilla/websocket dependency

- [ ] **Step 1: Run go mod tidy**

```bash
cd /home/li/Desktop/golang-learning
go mod tidy
```

- [ ] **Step 2: Verify gorilla/websocket is gone**

```bash
grep gorilla go.mod go.sum
```

Expected: no output.

- [ ] **Step 3: Full build + test**

```bash
go build ./...
go test ./...
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: remove gorilla/websocket dependency"
```

---

## Task 7: E2E test on prod Docker stack

- [ ] **Step 1: Build and start the prod stack**

```bash
cd /home/li/Desktop/golang-learning
make prod-up
```

Wait ~10 seconds for containers to stabilize.

- [ ] **Step 2: Run migration**

```bash
make prod-migrate
```

- [ ] **Step 3: Stream a message**

```bash
make chat USER=li SESSION=e2e-test-1 MSG="Tell me about Go channels" PORT=8000
```

Expected: NDJSON chunks appear in terminal, ending with `{"done":true,...}`.

- [ ] **Step 4: Check history is persisted**

```bash
make history SESSION=e2e-test-1 USER=li PORT=8000
```

Expected: JSON array with user message + assistant reply.

- [ ] **Step 5: Tear down**

```bash
make prod-down
```
