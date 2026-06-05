package usecase

import (
	"context"

	"golang-learning/shared"
)

type ProcessChatRequestUseCase struct {
	generator ITokenGenerator
	publisher IEventPublisher
}

func NewProcessChatRequest(
	generator ITokenGenerator,
	publisher IEventPublisher,
) *ProcessChatRequestUseCase {
	return &ProcessChatRequestUseCase{
		generator: generator,
		publisher: publisher,
	}
}

func (uc *ProcessChatRequestUseCase) Execute(ctx context.Context, req shared.ChatRequest) error {
	tokenCh, err := uc.generator.Generate(ctx, req.Content)
	if err != nil {
		return err
	}

	for token := range tokenCh {
		if err := uc.publisher.PublishToken(ctx, shared.TokenEvent{
			RequestID:   req.RequestID,
			SessionID:   req.SessionID,
			UserMessage: req.Content,
			Delta:       token,
			Done:        false,
		}); err != nil {
			return err
		}
	}

	return uc.publisher.PublishToken(ctx, shared.TokenEvent{
		RequestID:   req.RequestID,
		SessionID:   req.SessionID,
		UserMessage: req.Content,
		Delta:       "",
		Done:        true,
	})
}
