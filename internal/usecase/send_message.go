package usecase

import (
	"context"
	"fmt"

	"golang-learning/shared"
)

type SendMessageUseCase struct {
	publisher    IEventPublisher
	callbackBase string
}

func NewSendMessage(publisher IEventPublisher, callbackBase string) *SendMessageUseCase {
	return &SendMessageUseCase{publisher: publisher, callbackBase: callbackBase}
}

func (uc *SendMessageUseCase) Execute(ctx context.Context, sessionID, content, requestID string, out ISendMessageOutputPort) {
	req := shared.ChatRequest{
		RequestID:   requestID,
		SessionID:   sessionID,
		Content:     content,
		CallbackURL: fmt.Sprintf("%s/internal/tokens/%s", uc.callbackBase, requestID),
	}
	if err := uc.publisher.PublishRequest(ctx, req); err != nil {
		out.PresentError(err)
		return
	}
	out.PresentRequestID(requestID)
}
