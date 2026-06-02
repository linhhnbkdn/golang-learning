package usecase

import (
	"context"

	"golang-learning/internal/application/port"
	"golang-learning/internal/domain"
)

type GetHistoryUseCase struct {
	cache port.ConversationCache
}

func NewGetHistory(cache port.ConversationCache) *GetHistoryUseCase {
	return &GetHistoryUseCase{cache: cache}
}

func (uc *GetHistoryUseCase) Execute(ctx context.Context, sessionID string) ([]domain.Message, error) {
	return uc.cache.GetHistory(ctx, sessionID)
}
