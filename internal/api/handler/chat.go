package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"golang-learning/internal/api/middleware"
	"golang-learning/internal/api/state"
	"golang-learning/internal/application/port"
	"golang-learning/internal/application/usecase"
	"golang-learning/internal/domain"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type ChatHandler struct {
	sendMessage *usecase.SendMessageUseCase
	getHistory  *usecase.GetHistoryUseCase
	store       port.MessageStore
	ownerStore  port.SessionOwnerStore
	sseState    *state.SSEState
	log         *zap.Logger
}

func NewChatHandler(
	sendMessage *usecase.SendMessageUseCase,
	getHistory *usecase.GetHistoryUseCase,
	store port.MessageStore,
	ownerStore port.SessionOwnerStore,
	sseState *state.SSEState,
	log *zap.Logger,
) *ChatHandler {
	return &ChatHandler{
		sendMessage: sendMessage,
		getHistory:  getHistory,
		store:       store,
		ownerStore:  ownerStore,
		sseState:    sseState,
		log:         log,
	}
}

func (h *ChatHandler) RegisterRoutes(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	auth := r.Group("/", authMiddleware)
	auth.POST("/chat", h.PostChat)
	auth.GET("/chat/stream/:request_id", h.StreamResponse)
	auth.GET("/history/:session_id", h.GetHistory)
	auth.GET("/history/:session_id/db", h.GetHistoryDB)
}

type chatBody struct {
	SessionID string `json:"session_id" binding:"required"`
	Content   string `json:"content"    binding:"required"`
}

func (h *ChatHandler) PostChat(c *gin.Context) {
	var body chatBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userID := c.GetString(middleware.UserIDKey)
	ctx := c.Request.Context()

	requestID, err := h.sendMessage.Execute(ctx, body.SessionID, body.Content)
	if err != nil {
		h.log.Error("send message failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := h.ownerStore.SetOwner(ctx, body.SessionID, userID); err != nil {
		h.log.Warn("set session owner failed", zap.Error(err), zap.String("session_id", body.SessionID))
	}

	h.log.Info("chat request published",
		zap.String("request_id", requestID),
		zap.String("session_id", body.SessionID),
		zap.String("user_id", userID),
	)
	c.JSON(http.StatusOK, gin.H{"request_id": requestID})
}

func (h *ChatHandler) StreamResponse(c *gin.Context) {
	requestID := c.Param("request_id")
	ch := h.sseState.Register(requestID)
	defer h.sseState.Unregister(requestID)

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming not supported"})
		return
	}

	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if resp.FinishReason != nil && *resp.FinishReason == "stop" {
				fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
				flusher.Flush()
				return
			}
			payload, _ := json.Marshal(map[string]any{
				"choices": []map[string]any{
					{"delta": map[string]string{"content": resp.Delta}, "finish_reason": nil},
				},
			})
			fmt.Fprintf(c.Writer, "data: %s\n\n", payload)
			flusher.Flush()

		case <-time.After(30 * time.Second):
			fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
			flusher.Flush()
			return

		case <-c.Request.Context().Done():
			return
		}
	}
}

func (h *ChatHandler) GetHistory(c *gin.Context) {
	sessionID := c.Param("session_id")
	if h.guardSession(c, sessionID) {
		return
	}
	messages, err := h.getHistory.Execute(c.Request.Context(), sessionID)
	if err != nil {
		h.log.Error("get history failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, toResponse(messages))
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
	c.JSON(http.StatusOK, toResponse(messages))
}

// guardSession returns true (and writes response) if user does not own the session.
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

func toResponse(messages []domain.Message) []map[string]string {
	out := make([]map[string]string, 0, len(messages))
	for _, m := range messages {
		out = append(out, map[string]string{
			"role":       string(m.Role),
			"content":    m.Content,
			"request_id": m.RequestID,
		})
	}
	return out
}
