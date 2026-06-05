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

// streamToken chỉ chứa fields cần thiết cho gRPC delivery — bỏ UserMessage và SessionID
type streamToken struct {
	RequestID string `json:"request_id"`
	Delta     string `json:"delta"`
	Done      bool   `json:"done"`
}

type StreamingWorker struct {
	useCase *usecase.StreamTokensUseCase
	reader  *kafka.Reader

	mu       sync.Mutex
	channels map[string]chan streamToken
}

func NewStreamingWorker(cfg config.Config, useCase *usecase.StreamTokensUseCase) *StreamingWorker {
	return &StreamingWorker{
		useCase:  useCase,
		channels: make(map[string]chan streamToken),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.KafkaBrokers,
			GroupID:        "streaming-worker",
			Topic:          "stream-llm-fe",
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        10 * time.Millisecond,
			CommitInterval: time.Second,
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

		if err := w.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("streaming worker commit error", "err", err)
		}

		var token streamToken
		if err := json.Unmarshal(msg.Value, &token); err != nil {
			slog.Error("streaming worker unmarshal error", "err", err)
			continue
		}

		w.route(ctx, token)
	}
}

func (w *StreamingWorker) route(ctx context.Context, token streamToken) {
	w.mu.Lock()
	ch, exists := w.channels[token.RequestID]
	if !exists {
		ch = make(chan streamToken, 32)
		w.channels[token.RequestID] = ch
		go w.processRequest(ctx, token.RequestID, ch)
	}
	w.mu.Unlock()

	ch <- token
}

func (w *StreamingWorker) processRequest(ctx context.Context, requestID string, ch chan streamToken) {
	defer func() {
		w.mu.Lock()
		delete(w.channels, requestID)
		w.mu.Unlock()
	}()

	for token := range ch {
		if err := w.useCase.Execute(ctx, shared.TokenEvent{
			RequestID: token.RequestID,
			Delta:     token.Delta,
			Done:      token.Done,
		}); err != nil {
			slog.Error("streaming worker deliver error", "err", err, "request_id", requestID)
		}
		if token.Done {
			slog.Info("streaming worker request done", "request_id", requestID)
			return
		}
	}
}
