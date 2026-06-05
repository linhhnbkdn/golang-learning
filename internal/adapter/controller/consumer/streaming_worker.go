package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"golang-learning/config"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

type StreamingWorker struct {
	useCase *usecase.StreamTokensUseCase
	reader  *kafka.Reader

	mu       sync.Mutex
	channels map[string]chan shared.TokenEvent
}

func NewStreamingWorker(cfg config.Config, useCase *usecase.StreamTokensUseCase) *StreamingWorker {
	return &StreamingWorker{
		useCase:  useCase,
		channels: make(map[string]chan shared.TokenEvent),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.KafkaBrokers,
			GroupID:  "streaming-worker",
			Topic:    "stream-llm-fe",
			MinBytes: 1,
			MaxBytes: 10e6,
			MaxWait:  10 * time.Millisecond,
		}),
	}
}

func (w *StreamingWorker) Run(ctx context.Context) error {
	defer w.reader.Close()
	slog.Info("streaming worker started — listening on stream-llm-fe")

	for {
		msg, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("streaming worker read error", "err", err)
			continue
		}

		// Commit ngay — consistent với LLM worker (at-most-once)
		if err := w.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("streaming worker commit error", "err", err)
		}

		var token shared.TokenEvent
		if err := json.Unmarshal(msg.Value, &token); err != nil {
			slog.Error("streaming worker unmarshal error", "err", err)
			continue
		}

		w.route(ctx, token)
	}
}

// route gửi token vào channel của request tương ứng.
// Mỗi requestID có 1 goroutine riêng đảm bảo thứ tự gRPC send.
func (w *StreamingWorker) route(ctx context.Context, token shared.TokenEvent) {
	w.mu.Lock()
	ch, exists := w.channels[token.RequestID]
	if !exists {
		ch = make(chan shared.TokenEvent, 256)
		w.channels[token.RequestID] = ch
		go w.processRequest(ctx, token.RequestID, ch)
	}
	w.mu.Unlock()

	ch <- token
}

func (w *StreamingWorker) processRequest(ctx context.Context, requestID string, ch chan shared.TokenEvent) {
	defer func() {
		w.mu.Lock()
		delete(w.channels, requestID)
		w.mu.Unlock()
	}()

	for token := range ch {
		if err := w.useCase.Execute(ctx, token); err != nil {
			slog.Error("streaming worker deliver error", "err", err, "request_id", requestID)
		}
		if token.Done {
			return
		}
	}
}
