package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"golang-learning/config"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

type StreamingWorker struct {
	useCase *usecase.StreamTokensUseCase
	reader  *kafka.Reader
}

func NewStreamingWorker(cfg config.Config, useCase *usecase.StreamTokensUseCase) *StreamingWorker {
	return &StreamingWorker{
		useCase: useCase,
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

		var token shared.TokenEvent
		if err := json.Unmarshal(msg.Value, &token); err != nil {
			slog.Error("streaming worker unmarshal error", "err", err)
			if err := w.reader.CommitMessages(ctx, msg); err != nil {
				slog.Error("streaming worker commit error", "err", err)
			}
			continue
		}

		if err := w.useCase.Execute(ctx, token); err != nil {
			slog.Error("streaming worker deliver error", "err", err, "request_id", token.RequestID)
		}

		if err := w.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("streaming worker commit error", "err", err)
		}
	}
}
