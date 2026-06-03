package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"golang-learning/config"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

type PersistenceWorker struct {
	useCase *usecase.PersistSessionUseCase
	reader  *kafka.Reader
}

func NewPersistenceWorker(cfg config.Config, useCase *usecase.PersistSessionUseCase) *PersistenceWorker {
	return &PersistenceWorker{
		useCase: useCase,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.KafkaBrokers,
			GroupID:  "persistence-worker",
			Topic:    "chat.completed",
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

func (w *PersistenceWorker) Run(ctx context.Context) error {
	defer w.reader.Close()
	slog.Info("persistence worker started — listening on chat.completed")

	for {
		msg, err := w.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("persistence read error", "err", err)
			continue
		}

		var completed shared.ChatCompleted
		if err := json.Unmarshal(msg.Value, &completed); err != nil {
			slog.Error("persistence unmarshal error", "err", err)
			continue
		}

		slog.Info("persisting session", "session_id", completed.SessionID, "request_id", completed.RequestID)
		if err := w.useCase.Execute(ctx, completed); err != nil {
			slog.Error("persist session failed", "err", err, "request_id", completed.RequestID)
		}
	}
}
