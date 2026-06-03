package usecase

import (
	"context"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type ConversationCache interface {
	SaveMessage(ctx context.Context, msg entity.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error)
}

type EventPublisher interface {
	PublishRequest(ctx context.Context, req shared.ChatRequest) error
	PublishResponse(ctx context.Context, resp shared.ChatResponse) error
	PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error
	Flush()
}

type MessageStore interface {
	SaveMessage(ctx context.Context, msg entity.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error)
}

type TokenGenerator interface {
	Generate(ctx context.Context, content string) (<-chan string, error)
}

type SessionOwnerStore interface {
	// ClaimOwner atomically sets owner if not exists (SetNX).
	// Returns true if this user owns the session (claimed now or already owned by them).
	// Returns false if the session is owned by a different user.
	ClaimOwner(ctx context.Context, sessionID, userID string) (bool, error)
	GetOwner(ctx context.Context, sessionID string) (string, error)
}

type RequestOwnerStore interface {
	SetRequestOwner(ctx context.Context, requestID, userID string) error
	GetRequestOwner(ctx context.Context, requestID string) (string, error)
}
