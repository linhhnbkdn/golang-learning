package usecase

import (
	"context"

	"golang-learning/shared"
)

type SendMessageUseCase struct {
	publisher EventPublisher
}

func NewSendMessage(publisher EventPublisher) *SendMessageUseCase {
	return &SendMessageUseCase{publisher: publisher}
}

func (uc *SendMessageUseCase) Execute(ctx context.Context, sessionID, content string) (string, error) {
	req := shared.NewChatRequest(sessionID, content)
	if err := uc.publisher.PublishRequest(ctx, req); err != nil {
		return "", err
	}
	return req.RequestID, nil
}
