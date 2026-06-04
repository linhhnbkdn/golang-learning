package usecase

import (
	"context"
	"strings"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type ProcessChatRequestUseCase struct {
	generator ITokenGenerator
	publisher IEventPublisher
	cache     IConversationCache
	pubSub    IPubSubStream
}

func NewProcessChatRequest(
	generator ITokenGenerator,
	publisher IEventPublisher,
	cache IConversationCache,
	pubSub IPubSubStream,
) *ProcessChatRequestUseCase {
	return &ProcessChatRequestUseCase{
		generator: generator,
		publisher: publisher,
		cache:     cache,
		pubSub:    pubSub,
	}
}

func (uc *ProcessChatRequestUseCase) Execute(ctx context.Context, req shared.ChatRequest) error {
	fullResponse, err := uc.streamTokens(ctx, req)
	if err != nil {
		return err
	}

	if err := uc.cacheMessages(ctx, req, fullResponse); err != nil {
		return err
	}

	return uc.publisher.PublishCompleted(ctx, shared.ChatCompleted{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
	})
}

func (uc *ProcessChatRequestUseCase) streamTokens(ctx context.Context, req shared.ChatRequest) (string, error) {
	tokenCh, err := uc.generator.Generate(ctx, req.Content)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	for token := range tokenCh {
		sb.WriteString(token)
		if err := uc.pubSub.Publish(ctx, req.SessionID, req.RequestID, token, false); err != nil {
			return "", err
		}
	}

	return sb.String(), uc.pubSub.Publish(ctx, req.SessionID, req.RequestID, "", true)
}

func (uc *ProcessChatRequestUseCase) cacheMessages(ctx context.Context, req shared.ChatRequest, fullResponse string) error {
	if err := uc.cache.SaveMessage(ctx, entity.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      entity.RoleUser,
		Content:   req.Content,
	}); err != nil {
		return err
	}
	return uc.cache.SaveMessage(ctx, entity.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      entity.RoleAssistant,
		Content:   fullResponse,
	})
}
