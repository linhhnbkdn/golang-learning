package usecase

import (
	"context"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type PersistSessionUseCase struct {
	cache IConversationCache
	store IMessageStore
}

func NewPersistSession(cache IConversationCache, store IMessageStore) *PersistSessionUseCase {
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

func (uc *PersistSessionUseCase) ExecuteBatch(ctx context.Context, batch []shared.ChatCompleted) error {
	var all []entity.Message
	for _, completed := range batch {
		messages, err := uc.cache.GetHistory(ctx, completed.SessionID)
		if err != nil {
			return err
		}
		for _, msg := range messages {
			if msg.RequestID == completed.RequestID {
				all = append(all, msg)
			}
		}
	}
	if len(all) == 0 {
		return nil
	}
	if err := uc.store.BulkSaveMessages(ctx, all); err != nil {
		return err
	}
	seen := make(map[string]struct{})
	for _, completed := range batch {
		if _, ok := seen[completed.SessionID]; ok {
			continue
		}
		seen[completed.SessionID] = struct{}{}
		_ = uc.cache.DeleteSession(ctx, completed.SessionID)
	}
	return nil
}
