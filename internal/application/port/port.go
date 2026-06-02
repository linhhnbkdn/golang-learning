package port

import (
	"context"

	"golang-learning/internal/domain"
	"golang-learning/shared"
)

type ConversationCache interface {
	SaveMessage(ctx context.Context, msg domain.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]domain.Message, error)
}

type EventPublisher interface {
	PublishRequest(ctx context.Context, req shared.ChatRequest) error
	PublishResponse(ctx context.Context, resp shared.ChatResponse) error
	PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error
	Flush()
}

type MessageStore interface {
	SaveMessage(ctx context.Context, msg domain.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]domain.Message, error)
}

type TokenGenerator interface {
	Generate(ctx context.Context, content string) (<-chan string, error)
}
