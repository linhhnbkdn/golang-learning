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
	content     string // delta tokens kể từ lần flush trước
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

// Flush: snapshot buffer → đọc DB existing content → merge → upsert → clear buffer
func (uc *PersistSessionUseCase) Flush(ctx context.Context) error {
	uc.mu.Lock()
	if len(uc.buffers) == 0 {
		uc.mu.Unlock()
		return nil
	}
	snapshot := make(map[string]*tokenBuffer, len(uc.buffers))
	for id, buf := range uc.buffers {
		snapshot[id] = &tokenBuffer{
			sessionID:   buf.sessionID,
			userMessage: buf.userMessage,
			content:     buf.content,
		}
	}
	// Clear buffer sau khi snapshot
	uc.buffers = make(map[string]*tokenBuffer)
	uc.count = 0
	uc.mu.Unlock()

	// Đọc DB để lấy content hiện tại của assistant
	requestIDs := make([]string, 0, len(snapshot))
	for id := range snapshot {
		requestIDs = append(requestIDs, id)
	}
	existing, err := uc.store.GetContentByRequestIDs(ctx, requestIDs)
	if err != nil {
		return err
	}

	msgs := make([]entity.Message, 0, len(snapshot)*2)
	for requestID, buf := range snapshot {
		msgs = append(msgs,
			entity.Message{SessionID: buf.sessionID, RequestID: requestID, Role: entity.RoleUser, Content: buf.userMessage},
			entity.Message{SessionID: buf.sessionID, RequestID: requestID, Role: entity.RoleAssistant, Content: existing[requestID] + buf.content},
		)
	}

	return uc.store.BulkUpsertMessages(ctx, msgs)
}
