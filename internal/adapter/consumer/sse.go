package consumer

import (
	"context"
	"encoding/json"

	"golang-learning/internal/adapter/http/state"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ConsumeResponses reads from chat.responses and fans out to SSE connections.
func ConsumeResponses(ctx context.Context, r *kafka.Reader, s *state.SSEState, log *zap.Logger) {
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("response consumer error", zap.Error(err))
			continue
		}
		var resp shared.ChatResponse
		if err := json.Unmarshal(msg.Value, &resp); err != nil {
			log.Error("unmarshal response failed", zap.Error(err))
			continue
		}
		s.Route(resp)
	}
}
