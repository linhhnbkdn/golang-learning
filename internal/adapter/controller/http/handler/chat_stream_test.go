package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang-learning/config"
	"golang-learning/internal/adapter/controller/http/handler"
	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- mocks ---

type mockOwnerStore struct {
	owned bool
	err   error
}

func (m *mockOwnerStore) ClaimOwner(_ context.Context, _, _ string) (bool, error) {
	return m.owned, m.err
}

func (m *mockOwnerStore) GetOwner(_ context.Context, _ string) (string, error) {
	return "", nil
}

type mockTokenHub struct {
	tokens []usecase.PubSubToken
}

func (m *mockTokenHub) Register(_ string) (<-chan usecase.PubSubToken, func()) {
	ch := make(chan usecase.PubSubToken, len(m.tokens))
	for _, t := range m.tokens {
		ch <- t
	}
	close(ch)
	return ch, func() {}
}

func (m *mockTokenHub) Deliver(_ string, _ usecase.PubSubToken) {}

type mockEventPublisher struct{ err error }

func (m *mockEventPublisher) PublishRequest(_ context.Context, _ shared.ChatRequest) error {
	return m.err
}

func (m *mockEventPublisher) PublishCompleted(_ context.Context, _ shared.ChatCompleted) error {
	return nil
}

func (m *mockEventPublisher) PublishToken(_ context.Context, _ shared.TokenEvent) error {
	return nil
}

type mockCallbackStore struct{}

func (m *mockCallbackStore) SetCallback(_ context.Context, _, _ string) error { return nil }
func (m *mockCallbackStore) GetCallback(_ context.Context, _ string) (string, error) {
	return "", nil
}

// --- helper ---

func newTestRouter(tokens []usecase.PubSubToken, owned bool) *gin.Engine {
	gin.SetMode(gin.TestMode)

	ownerStore := &mockOwnerStore{owned: owned}
	hub := &mockTokenHub{tokens: tokens}
	publisher := &mockEventPublisher{}
	sendMessage := usecase.NewSendMessage(publisher, "test-api:50051")

	h := handler.NewChatStreamHandler(sendMessage, ownerStore, hub, &mockCallbackStore{}, config.Config{}, zap.NewNop())

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(middleware.UserIDKey, "user1")
		c.Next()
	})
	r.POST("/chat/:session_id", h.Stream)
	return r
}

// --- tests ---

func TestChatStreamHandler_StreamsTokens(t *testing.T) {
	tokens := []usecase.PubSubToken{
		{RequestID: "req1", Delta: "Hello", Done: false},
		{RequestID: "req1", Delta: " world", Done: false},
		{RequestID: "req1", Delta: "", Done: true},
	}

	r := newTestRouter(tokens, true)

	body := `{"content":"say hello"}`
	req := httptest.NewRequest(http.MethodPost, "/chat/sess1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

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
		require.NoError(t, json.Unmarshal([]byte(line), &c))
		chunks = append(chunks, c)
	}
	require.NoError(t, scanner.Err())

	require.Len(t, chunks, 3, "expected 3 NDJSON chunks")
	assert.Equal(t, "Hello", chunks[0].Delta)
	assert.False(t, chunks[0].Done)
	assert.Equal(t, " world", chunks[1].Delta)
	assert.False(t, chunks[1].Done)
	assert.True(t, chunks[2].Done)
}

func TestChatStreamHandler_ForbiddenWhenNotOwner(t *testing.T) {
	r := newTestRouter(nil, false)

	body := `{"content":"say hello"}`
	req := httptest.NewRequest(http.MethodPost, "/chat/sess1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestChatStreamHandler_BadRequestOnEmptyContent(t *testing.T) {
	r := newTestRouter(nil, true)

	body := `{"content":""}`
	req := httptest.NewRequest(http.MethodPost, "/chat/sess1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestChatStreamHandler_InternalErrorOnPublishFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ownerStore := &mockOwnerStore{owned: true}
	hub := &mockTokenHub{}
	publisher := &mockEventPublisher{err: errors.New("kafka unavailable")}
	sendMessage := usecase.NewSendMessage(publisher, "test-api:50051")

	h := handler.NewChatStreamHandler(sendMessage, ownerStore, hub, &mockCallbackStore{}, config.Config{}, zap.NewNop())

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(middleware.UserIDKey, "user1")
		c.Next()
	})
	r.POST("/chat/:session_id", h.Stream)

	body := `{"content":"say hello"}`
	req := httptest.NewRequest(http.MethodPost, "/chat/sess1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
