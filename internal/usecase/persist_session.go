package usecase

import (
	"context"
	"sync"

	"golang-learning/internal/entity"
	"golang-learning/shared"
)

type tokenBuffer struct {
	sessionID   string
	userMessage string
	content     string
}

type PersistSessionUseCase struct {
	store   IMessageStore
	mu      sync.Mutex
	buffers map[string]*tokenBuffer
	count   int
}

func NewPersistSession(store IMessageStore) *PersistSessionUseCase {
	return &PersistSessionUseCase{
		store:   store,
		buffers: make(map[string]*tokenBuffer),
	}
}

func (uc *PersistSessionUseCase) AddToken(token shared.TokenEvent) {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	buf, ok := uc.buffers[token.RequestID]
	if !ok {
		buf = &tokenBuffer{
			sessionID:   token.SessionID,
			userMessage: token.UserMessage,
		}
		uc.buffers[token.RequestID] = buf
	}
	buf.content += token.Delta
	uc.count++
}

func (uc *PersistSessionUseCase) ShouldFlush(threshold int) bool {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	return uc.count >= threshold
}

func (uc *PersistSessionUseCase) Flush(ctx context.Context) error {
	uc.mu.Lock()
	snapshot := uc.buffers
	uc.buffers = make(map[string]*tokenBuffer)
	uc.count = 0
	uc.mu.Unlock()

	if len(snapshot) == 0 {
		return nil
	}

	requestIDs := make([]string, 0, len(snapshot))
	for id := range snapshot {
		requestIDs = append(requestIDs, id)
	}

	existing, err := uc.store.GetContentByRequestIDs(ctx, requestIDs)
	if err != nil {
		return err
	}

	var msgs []entity.Message
	for requestID, buf := range snapshot {
		prev := existing[requestID]
		fullContent := prev + buf.content

		msgs = append(msgs, entity.Message{
			SessionID: buf.sessionID,
			RequestID: requestID,
			Role:      entity.RoleUser,
			Content:   buf.userMessage,
		})
		msgs = append(msgs, entity.Message{
			SessionID: buf.sessionID,
			RequestID: requestID,
			Role:      entity.RoleAssistant,
			Content:   fullContent,
		})
	}

	return uc.store.BulkUpsertMessages(ctx, msgs)
}
