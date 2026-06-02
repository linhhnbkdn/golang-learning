package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"golang-learning/internal/api/state"
	"golang-learning/internal/application/port"
	"golang-learning/internal/application/usecase"
	"golang-learning/shared"

	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	sendMessage *usecase.SendMessageUseCase
	getHistory  *usecase.GetHistoryUseCase
	store       port.MessageStore
	sseState    *state.SSEState
}

func NewChatHandler(
	sendMessage *usecase.SendMessageUseCase,
	getHistory *usecase.GetHistoryUseCase,
	store port.MessageStore,
	sseState *state.SSEState,
) *ChatHandler {
	return &ChatHandler{
		sendMessage: sendMessage,
		getHistory:  getHistory,
		store:       store,
		sseState:    sseState,
	}
}

func (h *ChatHandler) RegisterRoutes(r *gin.Engine) {
	r.POST("/chat", h.PostChat)
	r.GET("/chat/stream/:request_id", h.StreamResponse)
	r.GET("/history/:session_id", h.GetHistory)
	r.GET("/history/:session_id/db", h.GetHistoryDB)
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
	requestID, err := h.sendMessage.Execute(c.Request.Context(), body.SessionID, body.Content)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
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
	messages, err := h.getHistory.Execute(c.Request.Context(), sessionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	result := make([]shared.ChatResponse, 0, len(messages))
	_ = result
	out := make([]map[string]string, 0, len(messages))
	for _, m := range messages {
		out = append(out, map[string]string{
			"role":       string(m.Role),
			"content":    m.Content,
			"request_id": m.RequestID,
		})
	}
	c.JSON(http.StatusOK, out)
}

func (h *ChatHandler) GetHistoryDB(c *gin.Context) {
	sessionID := c.Param("session_id")
	messages, err := h.store.GetHistory(c.Request.Context(), sessionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	out := make([]map[string]string, 0, len(messages))
	for _, m := range messages {
		out = append(out, map[string]string{
			"role":       string(m.Role),
			"content":    m.Content,
			"request_id": m.RequestID,
		})
	}
	c.JSON(http.StatusOK, out)
}
