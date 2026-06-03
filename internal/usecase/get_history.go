package usecase

import (
	"context"
)

type GetHistoryUseCase struct {
	cache IConversationCache
}

func NewGetHistory(cache IConversationCache) *GetHistoryUseCase {
	return &GetHistoryUseCase{cache: cache}
}

func (uc *GetHistoryUseCase) Execute(ctx context.Context, sessionID string, out IGetHistoryOutputPort) {
	messages, err := uc.cache.GetHistory(ctx, sessionID)
	if err != nil {
		out.PresentError(err)
		return
	}
	out.PresentMessages(messages)
}
