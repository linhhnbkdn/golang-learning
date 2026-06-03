package usecase

import (
	"context"
)

type GetHistoryUseCase struct {
	cache ConversationCache
}

func NewGetHistory(cache ConversationCache) *GetHistoryUseCase {
	return &GetHistoryUseCase{cache: cache}
}

func (uc *GetHistoryUseCase) Execute(ctx context.Context, sessionID string, out GetHistoryOutputPort) {
	messages, err := uc.cache.GetHistory(ctx, sessionID)
	if err != nil {
		out.PresentError(err)
		return
	}
	out.PresentMessages(messages)
}
