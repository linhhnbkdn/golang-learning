package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang-learning/shared"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPersister struct {
	mu         sync.Mutex
	tokens     []shared.TokenEvent
	flushCount int
}

func (m *mockPersister) AddToken(token shared.TokenEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens = append(m.tokens, token)
}

func (m *mockPersister) ShouldFlush(threshold int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tokens) >= threshold
}

func (m *mockPersister) Flush(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCount++
	return nil
}

func TestRun_CallsAddTokenForEachMessage(t *testing.T) {
	payloads := []shared.TokenEvent{
		{RequestID: "r1", Delta: "hello"},
		{RequestID: "r1", Delta: " world"},
		{RequestID: "r2", Delta: "hi"},
	}
	mock := newMockReader(payloads)
	persister := &mockPersister{}

	w := &PersistenceWorker{
		reader:  mock,
		useCase: persister,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	require.Len(t, persister.tokens, 3)
	assert.Equal(t, "hello", persister.tokens[0].Delta)
	assert.Len(t, mock.commits, 3, "must commit each message immediately")
}

func TestRun_FlushesWhenThresholdReached(t *testing.T) {
	payloads := make([]shared.TokenEvent, persistTokenThreshold)
	for i := range payloads {
		payloads[i] = shared.TokenEvent{RequestID: "r1", Delta: "x"}
	}
	mock := newMockReader(payloads)
	persister := &mockPersister{}

	w := &PersistenceWorker{
		reader:  mock,
		useCase: persister,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	assert.Greater(t, persister.flushCount, 0, "flush must be called when threshold reached")
}
