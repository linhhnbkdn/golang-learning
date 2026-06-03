package usecase

import (
	"context"

	"golang-learning/shared"
)

type SendMessageUseCase struct {
	publisher IEventPublisher
}

func NewSendMessage(publisher IEventPublisher) *SendMessageUseCase {
	return &SendMessageUseCase{publisher: publisher}
}

func (uc *SendMessageUseCase) Execute(ctx context.Context, sessionID, content string, out ISendMessageOutputPort) {
	req := shared.NewChatRequest(sessionID, content)
	if err := uc.publisher.PublishRequest(ctx, req); err != nil {
		out.PresentError(err)
		return
	}
	out.PresentRequestID(req.RequestID)
}
