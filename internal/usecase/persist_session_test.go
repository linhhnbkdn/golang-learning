package usecase

import (
	"context"
	"testing"

	"golang-learning/internal/entity"
	"golang-learning/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCache struct {
	history  map[string][]entity.Message
	deleted  []string
}

func (m *mockCache) SaveMessage(_ context.Context, msg entity.Message) error {
	m.history[msg.SessionID] = append(m.history[msg.SessionID], msg)
	return nil
}

func (m *mockCache) GetHistory(_ context.Context, sessionID string) ([]entity.Message, error) {
	return m.history[sessionID], nil
}

func (m *mockCache) DeleteSession(_ context.Context, sessionID string) error {
	m.deleted = append(m.deleted, sessionID)
	return nil
}

type mockStore struct {
	saved []entity.Message
	bulk  [][]entity.Message
}

func (m *mockStore) SaveMessage(_ context.Context, msg entity.Message) error {
	m.saved = append(m.saved, msg)
	return nil
}

func (m *mockStore) BulkSaveMessages(_ context.Context, msgs []entity.Message) error {
	m.bulk = append(m.bulk, msgs)
	return nil
}

func (m *mockStore) GetHistory(_ context.Context, _ string) ([]entity.Message, error) {
	return nil, nil
}

func TestExecuteBatch_SavesMatchingMessages(t *testing.T) {
	cache := &mockCache{
		history: map[string][]entity.Message{
			"sess-1": {
				{SessionID: "sess-1", RequestID: "req-1", Role: entity.RoleUser, Content: "hi"},
				{SessionID: "sess-1", RequestID: "req-1", Role: entity.RoleAssistant, Content: "hello"},
				{SessionID: "sess-1", RequestID: "req-old", Role: entity.RoleUser, Content: "old"},
			},
			"sess-2": {
				{SessionID: "sess-2", RequestID: "req-2", Role: entity.RoleUser, Content: "hey"},
				{SessionID: "sess-2", RequestID: "req-2", Role: entity.RoleAssistant, Content: "hi there"},
			},
		},
	}
	store := &mockStore{}
	uc := NewPersistSession(cache, store)

	batch := []shared.ChatCompleted{
		{SessionID: "sess-1", RequestID: "req-1"},
		{SessionID: "sess-2", RequestID: "req-2"},
	}

	err := uc.ExecuteBatch(context.Background(), batch)
	require.NoError(t, err)

	require.Len(t, store.bulk, 1, "must call BulkSaveMessages once")
	saved := store.bulk[0]
	assert.Len(t, saved, 4, "2 messages per session, old request excluded")

	for _, msg := range saved {
		assert.NotEqual(t, "req-old", msg.RequestID, "must not save old request messages")
	}

	assert.ElementsMatch(t, []string{"sess-1", "sess-2"}, cache.deleted, "must delete Redis cache for each session after save")
}

func TestExecuteBatch_EmptyBatch(t *testing.T) {
	store := &mockStore{}
	uc := NewPersistSession(&mockCache{history: map[string][]entity.Message{}}, store)

	err := uc.ExecuteBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, store.bulk, "must not call BulkSaveMessages on empty batch")
}
