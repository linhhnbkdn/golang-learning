package consumer

import (
	"context"
	"encoding/json"

	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

type mockReader struct {
	msgs    []kafka.Message
	pos     int
	commits [][]kafka.Message
}

func newMockReader(payloads []shared.TokenEvent) *mockReader {
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
