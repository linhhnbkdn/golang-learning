package usecase

import (
	"context"
	"strings"

	"golang-learning/internal/application/port"
	"golang-learning/internal/domain"
	"golang-learning/shared"
)

type ProcessChatRequestUseCase struct {
	generator port.TokenGenerator
	publisher port.EventPublisher
	cache     port.ConversationCache
}

func NewProcessChatRequest(
	generator port.TokenGenerator,
	publisher port.EventPublisher,
	cache port.ConversationCache,
) *ProcessChatRequestUseCase {
	return &ProcessChatRequestUseCase{
		generator: generator,
		publisher: publisher,
		cache:     cache,
	}
}

func (uc *ProcessChatRequestUseCase) Execute(ctx context.Context, req shared.ChatRequest) error {
	fullResponse, err := uc.streamTokens(ctx, req)
	if err != nil {
		return err
	}
	uc.publisher.Flush()

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
		if err := uc.publisher.PublishResponse(ctx, shared.ChatResponse{
			RequestID: req.RequestID,
			SessionID: req.SessionID,
			Delta:     token,
		}); err != nil {
			return "", err
		}
	}

	stop := "stop"
	if err := uc.publisher.PublishResponse(ctx, shared.ChatResponse{
		RequestID:    req.RequestID,
		SessionID:    req.SessionID,
		Delta:        "",
		FinishReason: &stop,
	}); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func (uc *ProcessChatRequestUseCase) cacheMessages(ctx context.Context, req shared.ChatRequest, fullResponse string) error {
	if err := uc.cache.SaveMessage(ctx, domain.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      domain.RoleUser,
		Content:   req.Content,
	}); err != nil {
		return err
	}
	return uc.cache.SaveMessage(ctx, domain.Message{
		SessionID: req.SessionID,
		RequestID: req.RequestID,
		Role:      domain.RoleAssistant,
		Content:   fullResponse,
	})
}
