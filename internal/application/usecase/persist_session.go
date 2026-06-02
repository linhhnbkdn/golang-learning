package usecase

import (
	"context"

	"golang-learning/internal/application/port"
	"golang-learning/shared"
)

type PersistSessionUseCase struct {
	cache port.ConversationCache
	store port.MessageStore
}

func NewPersistSession(cache port.ConversationCache, store port.MessageStore) *PersistSessionUseCase {
	return &PersistSessionUseCase{cache: cache, store: store}
}

func (uc *PersistSessionUseCase) Execute(ctx context.Context, completed shared.ChatCompleted) error {
	messages, err := uc.cache.GetHistory(ctx, completed.SessionID)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		if msg.RequestID != completed.RequestID {
			continue
		}
		if err := uc.store.SaveMessage(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}
