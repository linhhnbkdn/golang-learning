package shared

import (
	"time"

	"github.com/google/uuid"
)

type ChatRequest struct {
	RequestID string  `json:"request_id"`
	SessionID string  `json:"session_id"`
	Content   string  `json:"content"`
	Timestamp float64 `json:"timestamp"`
}

func NewChatRequest(sessionID, content string) ChatRequest {
	return ChatRequest{
		RequestID: uuid.New().String(),
		SessionID: sessionID,
		Content:   content,
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
	}
}

type ChatResponse struct {
	RequestID    string  `json:"request_id"`
	SessionID    string  `json:"session_id"`
	Delta        string  `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type ChatCompleted struct {
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

type TokenEvent struct {
	RequestID   string `json:"request_id"`
	SessionID   string `json:"session_id"`
	UserMessage string `json:"user_message"`
	Delta       string `json:"delta"`
	Done        bool   `json:"done"`
}
