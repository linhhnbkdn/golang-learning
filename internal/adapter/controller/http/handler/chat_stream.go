package handler

import (
	"encoding/json"
	"net/http"

	"golang-learning/internal/adapter/controller/http/middleware"
	"golang-learning/internal/usecase"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ChatStreamHandler handles the POST /chat/:session_id streaming endpoint.
type ChatStreamHandler struct {
	sendMessage *usecase.SendMessageUseCase
	ownerStore  usecase.ISessionOwnerStore
	pubSub      usecase.IPubSubStream
	log         *zap.Logger
}

// NewChatStreamHandler constructs a ChatStreamHandler.
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

// RegisterRoutes registers POST /chat/:session_id behind the given auth middleware.
func (h *ChatStreamHandler) RegisterRoutes(r *gin.Engine, authMiddleware gin.HandlerFunc) {
	auth := r.Group("/", authMiddleware)
	auth.POST("/chat/:session_id", h.Stream)
}

// streamOutputPort captures the request ID (or error) from SendMessageUseCase.Execute.
type streamOutputPort struct {
	requestID string
	err       error
}

func (p *streamOutputPort) PresentRequestID(requestID string) { p.requestID = requestID }
func (p *streamOutputPort) PresentError(err error)            { p.err = err }

// tokenChunk is the NDJSON shape written to the response.
type tokenChunk struct {
	RequestID string `json:"request_id"`
	Delta     string `json:"delta"`
	Done      bool   `json:"done"`
}

// Stream handles POST /chat/:session_id — claims ownership, subscribes to the
// pub/sub stream, triggers message processing, and streams NDJSON tokens back.
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

	tokenCh, cleanup, err := h.pubSub.Subscribe(c.Request.Context(), sessionID)
	if err != nil {
		h.log.Error("pubsub subscribe failed", zap.String("session_id", sessionID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "subscribe failed"})
		return
	}
	// cleanup closes the Redis subscription; safe to call with unconsumed items in tokenCh
	// because ps.Close() causes the forwarding goroutine to exit via the msgs channel closing.
	defer cleanup()

	out := &streamOutputPort{}
	h.sendMessage.Execute(c.Request.Context(), sessionID, body.Content, out)
	if out.err != nil {
		h.log.Error("sendMessage execute failed", zap.String("session_id", sessionID), zap.Error(out.err))
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
