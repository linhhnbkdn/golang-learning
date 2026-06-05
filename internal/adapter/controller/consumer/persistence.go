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

const (
	persistBatchSize    = 500
	persistFlushTimeout = 2 * time.Second
)

type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type batchPersister interface {
	ExecuteBatch(ctx context.Context, batch []shared.ChatCompleted) error
}

type PersistenceWorker struct {
	useCase      batchPersister
	reader       kafkaReader
	flushTimeout time.Duration
}

func NewPersistenceWorker(cfg config.Config, useCase *usecase.PersistSessionUseCase) *PersistenceWorker {
	return &PersistenceWorker{
		useCase:      useCase,
		flushTimeout: persistFlushTimeout,
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

	for ctx.Err() == nil {
		batch, msgs := w.fetchBatch(ctx)
		if len(batch) == 0 {
			continue
		}
		if err := w.useCase.ExecuteBatch(ctx, batch); err != nil {
			slog.Error("bulk persist failed — not committing", "err", err, "count", len(batch))
			continue
		}
		if err := w.reader.CommitMessages(ctx, msgs...); err != nil {
			slog.Error("persistence commit error", "err", err)
		}
		slog.Info("batch persisted", "count", len(batch))
	}
	return nil
}

func (w *PersistenceWorker) fetchBatch(ctx context.Context) ([]shared.ChatCompleted, []kafka.Message) {
	deadline := time.Now().Add(w.flushTimeout)
	var (
		batch []shared.ChatCompleted
		msgs  []kafka.Message
	)

	for len(batch) < persistBatchSize {
		fetchCtx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := w.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			break
		}
		var completed shared.ChatCompleted
		if err := json.Unmarshal(msg.Value, &completed); err != nil {
			slog.Error("persistence unmarshal error", "err", err)
		} else {
			batch = append(batch, completed)
		}
		msgs = append(msgs, msg)
	}

	return batch, msgs
}
