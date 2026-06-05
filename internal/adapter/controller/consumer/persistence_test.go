package consumer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockReader feeds pre-encoded messages then blocks until ctx is cancelled.
type mockReader struct {
	msgs    []kafka.Message
	pos     int
	commits [][]kafka.Message
}

func newMockReader(payloads []shared.ChatCompleted) *mockReader {
	msgs := make([]kafka.Message, len(payloads))
	for i, p := range payloads {
		b, _ := json.Marshal(p)
		msgs[i] = kafka.Message{Value: b}
	}
	return &mockReader{msgs: msgs}
}

func (r *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.pos < len(r.msgs) {
		msg := r.msgs[r.pos]
		r.pos++
		return msg, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *mockReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.commits = append(r.commits, msgs)
	return nil
}

func (r *mockReader) Close() error { return nil }

// --- fetchBatch tests ---

func TestFetchBatch_StopsAtBatchSize(t *testing.T) {
	payloads := make([]shared.ChatCompleted, persistBatchSize+50)
	for i := range payloads {
		payloads[i] = shared.ChatCompleted{SessionID: "s", RequestID: "r"}
	}

	w := &PersistenceWorker{
		reader:       newMockReader(payloads),
		flushTimeout: 5 * time.Second,
	}

	batch, _ := w.fetchBatch(context.Background())

	require.Len(t, batch, persistBatchSize, "must stop at batch size limit")
}

func TestFetchBatch_FlushesOnTimeout(t *testing.T) {
	payloads := []shared.ChatCompleted{
		{SessionID: "s1", RequestID: "r1"},
		{SessionID: "s2", RequestID: "r2"},
		{SessionID: "s3", RequestID: "r3"},
	}

	w := &PersistenceWorker{
		reader:       newMockReader(payloads),
		flushTimeout: 100 * time.Millisecond,
	}

	start := time.Now()
	batch, _ := w.fetchBatch(context.Background())
	elapsed := time.Since(start)

	require.Len(t, batch, 3, "must return all 3 messages before timeout")
	assert.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "must wait for timeout")
}

func TestFetchBatch_ReturnsRawMessagesForCallerToCommit(t *testing.T) {
	payloads := make([]shared.ChatCompleted, 10)
	for i := range payloads {
		payloads[i] = shared.ChatCompleted{SessionID: "s", RequestID: "r"}
	}

	mock := newMockReader(payloads)
	w := &PersistenceWorker{
		reader:       mock,
		flushTimeout: 50 * time.Millisecond,
	}

	batch, msgs := w.fetchBatch(context.Background())

	require.Len(t, batch, 10)
	assert.Len(t, msgs, 10, "must return 10 raw messages for caller to commit")
	assert.Empty(t, mock.commits, "fetchBatch must NOT commit — caller's responsibility")
}
