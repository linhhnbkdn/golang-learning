package usecase

import (
	"context"
	"testing"

	"golang-learning/internal/entity"
	"golang-learning/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestExecuteBatch_SavesMessagesFromEvent(t *testing.T) {
	store := &mockStore{}
	uc := NewPersistSession(store)

	batch := []shared.ChatCompleted{
		{
			SessionID: "sess-1",
			RequestID: "req-1",
			Messages: []shared.ChatCompletedMessage{
				{Role: "user", Content: "hi"},
				{Role: "assistant", Content: "hello"},
			},
		},
		{
			SessionID: "sess-2",
			RequestID: "req-2",
			Messages: []shared.ChatCompletedMessage{
				{Role: "user", Content: "hey"},
				{Role: "assistant", Content: "hi there"},
			},
		},
	}

	err := uc.ExecuteBatch(context.Background(), batch)
	require.NoError(t, err)

	require.Len(t, store.bulk, 1, "must call BulkSaveMessages once")
	saved := store.bulk[0]
	assert.Len(t, saved, 4, "2 messages per session")

	assert.Equal(t, entity.RoleUser, saved[0].Role)
	assert.Equal(t, "hi", saved[0].Content)
	assert.Equal(t, "sess-1", saved[0].SessionID)
	assert.Equal(t, "req-1", saved[0].RequestID)
}

func TestExecuteBatch_EmptyBatch(t *testing.T) {
	store := &mockStore{}
	uc := NewPersistSession(store)

	err := uc.ExecuteBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, store.bulk, "must not call BulkSaveMessages on empty batch")
}

func TestExecuteBatch_EmptyMessages(t *testing.T) {
	store := &mockStore{}
	uc := NewPersistSession(store)

	batch := []shared.ChatCompleted{
		{SessionID: "sess-1", RequestID: "req-1", Messages: nil},
	}

	err := uc.ExecuteBatch(context.Background(), batch)
	require.NoError(t, err)
	assert.Empty(t, store.bulk, "must not call BulkSaveMessages when no messages in event")
}
