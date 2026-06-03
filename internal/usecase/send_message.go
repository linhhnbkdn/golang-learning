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

func (uc *SendMessageUseCase) Execute(ctx context.Context, sessionID, content string, out SendMessageOutputPort) {
	req := shared.NewChatRequest(sessionID, content)
	if err := uc.publisher.PublishRequest(ctx, req); err != nil {
		out.PresentError(err)
		return
	}
	out.PresentRequestID(req.RequestID)
}
