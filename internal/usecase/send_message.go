package usecase

import (
	"context"

	"golang-learning/shared"
)

type SendMessageUseCase struct {
	publisher IEventPublisher
}

func NewSendMessage(publisher IEventPublisher, _ string) *SendMessageUseCase {
	return &SendMessageUseCase{publisher: publisher}
}

func (uc *SendMessageUseCase) Execute(ctx context.Context, sessionID, content, requestID string, out ISendMessageOutputPort) {
	req := shared.ChatRequest{
		RequestID: requestID,
		SessionID: sessionID,
		Content:   content,
	}
	if err := uc.publisher.PublishRequest(ctx, req); err != nil {
		out.PresentError(err)
		return
	}
	out.PresentRequestID(requestID)
}
