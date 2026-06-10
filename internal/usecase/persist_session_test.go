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
	existing map[string]string
	upserted []entity.Message
}

func newMockStore() *mockStore {
	return &mockStore{existing: make(map[string]string)}
}

func (m *mockStore) SaveMessage(_ context.Context, _ entity.Message) error        { return nil }
func (m *mockStore) BulkSaveMessages(_ context.Context, _ []entity.Message) error { return nil }
func (m *mockStore) GetHistory(_ context.Context, _ string) ([]entity.Message, error) {
	return nil, nil
}
func (m *mockStore) GetContentByRequestIDs(_ context.Context, _ []string) (map[string]string, error) {
	return m.existing, nil
}
func (m *mockStore) BulkUpsertMessages(_ context.Context, msgs []entity.Message) error {
	m.upserted = append(m.upserted, msgs...)
	return nil
}

func TestFlush_MergesTokensAndPersists(t *testing.T) {
	store := newMockStore()
	uc := NewPersistSession(store)

	uc.AddToken(shared.TokenEvent{RequestID: "r1", SessionID: "s1", UserMessage: "hello user", Delta: "foo"})
	uc.AddToken(shared.TokenEvent{RequestID: "r1", SessionID: "s1", UserMessage: "hello user", Delta: "bar"})

	err := uc.Flush(context.Background())
	require.NoError(t, err)

	require.Len(t, store.upserted, 2, "must upsert user + assistant message")

	var userMsg, assistantMsg entity.Message
	for _, m := range store.upserted {
		if m.Role == entity.RoleUser {
			userMsg = m
		} else {
			assistantMsg = m
		}
	}

	assert.Equal(t, "hello user", userMsg.Content)
	assert.Equal(t, "r1", userMsg.RequestID)
	assert.Equal(t, "foobar", assistantMsg.Content)
	assert.Equal(t, "r1", assistantMsg.RequestID)
}

func TestFlush_EmptyBufferIsNoop(t *testing.T) {
	store := newMockStore()
	uc := NewPersistSession(store)

	err := uc.Flush(context.Background())
	require.NoError(t, err)
	assert.Empty(t, store.upserted)
}

func TestShouldFlush_ThresholdBehavior(t *testing.T) {
	store := newMockStore()
	uc := NewPersistSession(store)

	uc.AddToken(shared.TokenEvent{RequestID: "r1", Delta: "a"})
	assert.False(t, uc.ShouldFlush(2))

	uc.AddToken(shared.TokenEvent{RequestID: "r1", Delta: "b"})
	assert.True(t, uc.ShouldFlush(2))
}

func TestFlush_ClearsBufferAfterFlush(t *testing.T) {
	store := newMockStore()
	uc := NewPersistSession(store)

	uc.AddToken(shared.TokenEvent{RequestID: "r1", SessionID: "s1", UserMessage: "u", Delta: "x"})
	require.NoError(t, uc.Flush(context.Background()))

	// second flush on empty buffer must be noop
	store.upserted = nil
	require.NoError(t, uc.Flush(context.Background()))
	assert.Empty(t, store.upserted)
}
