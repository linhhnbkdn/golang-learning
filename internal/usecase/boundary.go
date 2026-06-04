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

type SSEToken struct {
	ID    string
	Delta string
	Done  bool
}

type ISSEStream interface {
	Publish(ctx context.Context, requestID, delta string) error
	PublishDone(ctx context.Context, requestID string) error
	Read(ctx context.Context, requestID, lastID string) ([]SSEToken, error)
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

type IRequestOwnerStore interface {
	SetRequestOwner(ctx context.Context, requestID, userID string) error
	GetRequestOwner(ctx context.Context, requestID string) (string, error)
}

type ISendMessageOutputPort interface {
	PresentRequestID(requestID string)
	PresentError(err error)
}

type IGetHistoryOutputPort interface {
	PresentMessages(messages []entity.Message)
	PresentError(err error)
}
