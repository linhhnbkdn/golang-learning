package usecase

import (
	"context"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type IConversationCache interface {
	SaveMessage(ctx context.Context, msg entity.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error)
	DeleteSession(ctx context.Context, sessionID string) error
}

type IEventPublisher interface {
	PublishRequest(ctx context.Context, req shared.ChatRequest) error
	PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error
	PublishToken(ctx context.Context, token shared.TokenEvent) error
}

type ICallbackStore interface {
	SetCallback(ctx context.Context, requestID, grpcAddr string) error
	GetCallback(ctx context.Context, requestID string) (string, error)
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

type ITokenHub interface {
	Register(requestID string) (<-chan PubSubToken, func())
	Deliver(requestID string, token PubSubToken)
}

type IMessageStore interface {
	SaveMessage(ctx context.Context, msg entity.Message) error
	BulkSaveMessages(ctx context.Context, msgs []entity.Message) error
	BulkUpsertMessages(ctx context.Context, msgs []entity.Message) error
	GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error)
	GetContentByRequestIDs(ctx context.Context, requestIDs []string) (map[string]string, error)
}

type ITokenGenerator interface {
	Generate(ctx context.Context, content string) (<-chan string, error)
}

type ISessionOwnerStore interface {
	ClaimOwner(ctx context.Context, sessionID, userID string) (bool, error)
	GetOwner(ctx context.Context, sessionID string) (string, error)
}

type ISendMessageOutputPort interface {
	PresentRequestID(requestID string)
	PresentError(err error)
}

type IGetHistoryOutputPort interface {
	PresentMessages(messages []entity.Message)
	PresentError(err error)
}
