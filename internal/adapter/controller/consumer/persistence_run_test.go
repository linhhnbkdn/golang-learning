package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang-learning/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPersister struct {
	err      error
	calls    [][]shared.ChatCompleted
}

func (m *mockPersister) ExecuteBatch(_ context.Context, batch []shared.ChatCompleted) error {
	m.calls = append(m.calls, batch)
	return m.err
}

func TestRun_CommitsAfterSuccessfulSave(t *testing.T) {
	payloads := []shared.ChatCompleted{
		{SessionID: "s1", RequestID: "r1"},
		{SessionID: "s2", RequestID: "r2"},
	}
	mock := newMockReader(payloads)
	persister := &mockPersister{}

	w := &PersistenceWorker{
		reader:       mock,
		useCase:      persister,
		flushTimeout: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	require.Len(t, persister.calls, 1, "ExecuteBatch must be called once")
	assert.Len(t, persister.calls[0], 2)
	require.Len(t, mock.commits, 1, "must commit after successful save")
	assert.Len(t, mock.commits[0], 2, "must commit both messages")
}

func TestRun_DoesNotCommitWhenSaveFails(t *testing.T) {
	payloads := []shared.ChatCompleted{
		{SessionID: "s1", RequestID: "r1"},
	}
	mock := newMockReader(payloads)
	persister := &mockPersister{err: errors.New("db down")}

	w := &PersistenceWorker{
		reader:       mock,
		useCase:      persister,
		flushTimeout: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	require.Len(t, persister.calls, 1, "ExecuteBatch must be called once")
	assert.Empty(t, mock.commits, "must NOT commit when DB save fails")
}
