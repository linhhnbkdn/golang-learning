package usecase

import (
	"context"

	"golang-learning/internal/domain"
)

type GetHistoryUseCase struct {
	cache ConversationCache
}

func NewGetHistory(cache ConversationCache) *GetHistoryUseCase {
	return &GetHistoryUseCase{cache: cache}
}

func (uc *GetHistoryUseCase) Execute(ctx context.Context, sessionID string) ([]domain.Message, error) {
	return uc.cache.GetHistory(ctx, sessionID)
}
