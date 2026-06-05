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
	persistTokenThreshold = 500
	persistFlushInterval  = 1 * time.Minute
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
			Topic:    "persistence-llm",
			MinBytes: 10e3,
			MaxBytes: 10e6,
			MaxWait:  500 * time.Millisecond,
		}),
	}
}

func (w *PersistenceWorker) Run(ctx context.Context) error {
	defer w.reader.Close()
	slog.Info("persistence worker started — listening on persistence-llm")

	var pending []kafka.Message
	ticker := time.NewTicker(persistFlushInterval)
	defer ticker.Stop()

	flush := func() {
		if err := w.useCase.Flush(ctx); err != nil {
			slog.Error("persistence flush failed", "err", err)
			return
		}
		if len(pending) > 0 {
			if err := w.reader.CommitMessages(ctx, pending...); err != nil {
				slog.Error("persistence commit error", "err", err)
			}
			slog.Info("persistence flushed", "messages", len(pending))
			pending = pending[:0]
		}
	}

	for {
		fetchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := w.reader.FetchMessage(fetchCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				flush()
				return nil
			}
			select {
			case <-ticker.C:
				flush()
			default:
			}
			continue
		}

		var token shared.TokenEvent
		if err := json.Unmarshal(msg.Value, &token); err != nil {
			slog.Error("persistence unmarshal error", "err", err)
			pending = append(pending, msg)
			continue
		}

		w.useCase.AddToken(token)
		pending = append(pending, msg)

		select {
		case <-ticker.C:
			flush()
		default:
			if w.useCase.ShouldFlush(persistTokenThreshold) {
				flush()
				ticker.Reset(persistFlushInterval)
			}
		}
	}
}
