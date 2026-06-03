package usecase

import (
	"context"

	"golang-learning/internal/entity"
)

type GetHistoryUseCase struct {
	cache ConversationCache
}

func NewGetHistory(cache ConversationCache) *GetHistoryUseCase {
	return &GetHistoryUseCase{cache: cache}
}

func (uc *GetHistoryUseCase) Execute(ctx context.Context, sessionID string) ([]entity.Message, error) {
	return uc.cache.GetHistory(ctx, sessionID)
}
