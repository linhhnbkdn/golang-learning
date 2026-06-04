package handler

import (
	"encoding/json"
	"net/http"

	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type ChatStreamHandler struct {
	sendMessage *usecase.SendMessageUseCase
	ownerStore  usecase.ISessionOwnerStore
	hub         usecase.ITokenHub
	log         *zap.Logger
}

func NewChatStreamHandler(
	sendMessage *usecase.SendMessageUseCase,
	ownerStore usecase.ISessionOwnerStore,
	hub usecase.ITokenHub,
	log *zap.Logger,
) *ChatStreamHandler {
	return &ChatStreamHandler{
		sendMessage: sendMessage,
		ownerStore:  ownerStore,
		hub:         hub,
		log:         log,
	}
}

func (h *ChatStreamHandler) RegisterRoutes(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	auth := r.Group("/", authMiddleware)
	auth.POST("/chat/:session_id", h.Stream)
}

type streamOutputPort struct {
	requestID string
	err       error
}

func (p *streamOutputPort) PresentRequestID(requestID string) { p.requestID = requestID }
func (p *streamOutputPort) PresentError(err error)            { p.err = err }

type tokenChunk struct {
	RequestID string `json:"request_id"`
	Delta     string `json:"delta"`
	Done      bool   `json:"done"`
}

func (h *ChatStreamHandler) Stream(c *gin.Context) {
	sessionID := c.Param("session_id")
	userID := c.GetString(middleware.UserIDKey)

	var body struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Content == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "content is required"})
		return
	}

	owned, err := h.ownerStore.ClaimOwner(c.Request.Context(), sessionID, userID)
	if err != nil || !owned {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return
	}

	// Register channel BEFORE publishing to Kafka — eliminates race with Worker callback
	requestID := uuid.New().String()
	tokenCh, cleanup := h.hub.Register(requestID)
	defer cleanup()

	out := &streamOutputPort{}
	h.sendMessage.Execute(c.Request.Context(), sessionID, body.Content, requestID, out)
	if out.err != nil {
		h.log.Error("sendMessage failed", zap.String("session_id", sessionID), zap.Error(out.err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": out.err.Error()})
		return
	}

	c.Header("Content-Type", "application/x-ndjson")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)

	enc := json.NewEncoder(c.Writer)
	flusher, canFlush := c.Writer.(http.Flusher)

	for token := range tokenCh {
		chunk := tokenChunk{
			RequestID: token.RequestID,
			Delta:     token.Delta,
			Done:      token.Done,
		}
		if err := enc.Encode(chunk); err != nil {
			return
		}
		if canFlush {
			flusher.Flush()
		}
		if token.Done {
			return
		}
	}
}
