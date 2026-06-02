package api

import (
	"context"
	"encoding/json"
	"log/slog"

	"golang-learning/internal/api/state"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

// StartResponseConsumer consumes chat.responses and fans out to SSE connections.
func StartResponseConsumer(ctx context.Context, brokers []string, sseState *state.SSEState) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "api-sse-fan-out",
		Topic:    "chat.responses",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go func() {
		defer r.Close()
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("response consumer error", "err", err)
				continue
			}
			var resp shared.ChatResponse
			if err := json.Unmarshal(msg.Value, &resp); err != nil {
				slog.Error("unmarshal response", "err", err)
				continue
			}
			sseState.Route(resp)
		}
	}()
}
