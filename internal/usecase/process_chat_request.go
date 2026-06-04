package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

var httpClient = &http.Client{Timeout: 5 * time.Second}

type ProcessChatRequestUseCase struct {
	generator      ITokenGenerator
	publisher      IEventPublisher
	cache          IConversationCache
	callbackBase   string
	callbackSecret string
}

func NewProcessChatRequest(
	generator ITokenGenerator,
	publisher IEventPublisher,
	cache IConversationCache,
	callbackBase string,
	callbackSecret string,
) *ProcessChatRequestUseCase {
	return &ProcessChatRequestUseCase{
		generator:      generator,
		publisher:      publisher,
		cache:          cache,
		callbackBase:   callbackBase,
		callbackSecret: callbackSecret,
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

	callbackURL := fmt.Sprintf("%s/internal/tokens/%s", uc.callbackBase, req.RequestID)

	var sb strings.Builder
	for token := range tokenCh {
		sb.WriteString(token)
		if err := uc.postToken(ctx, callbackURL, req.RequestID, token, false); err != nil {
			return "", err
		}
	}

	return sb.String(), uc.postToken(ctx, callbackURL, req.RequestID, "", true)
}

func (uc *ProcessChatRequestUseCase) postToken(ctx context.Context, callbackURL, requestID, delta string, done bool) error {
	body, _ := json.Marshal(PubSubToken{
		RequestID: requestID,
		Delta:     delta,
		Done:      done,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+uc.callbackSecret)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("callback returned %d", resp.StatusCode)
	}
	return nil
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
