package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"golang-learning/config"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

const (
	persistTokenThreshold = 500
	persistFlushInterval  = 30 * time.Second
)

type PersistenceWorker struct {
	useCase   *usecase.PersistSessionUseCase
	reader    *kafka.Reader
	flushing  atomic.Bool // prevent concurrent flushes
}

func NewPersistenceWorker(cfg config.Config, useCase *usecase.PersistSessionUseCase) *PersistenceWorker {
	return &PersistenceWorker{
		useCase: useCase,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.KafkaBrokers,
			GroupID:        "persistence-worker",
			Topic:          "persistence-llm",
			MinBytes:       10e3,
			MaxBytes:       10e6,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: time.Second,
		}),
	}
}

func (w *PersistenceWorker) Run(ctx context.Context) error {
	defer w.reader.Close()
	slog.Info("persistence worker started — listening on persistence-llm")

	// Flusher goroutine: không block consumer
	go w.runFlusher(ctx)

	for {
		msg, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("persistence read error", "err", err)
			continue
		}

		if err := w.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("persistence commit error", "err", err)
		}

		var token shared.TokenEvent
		if err := json.Unmarshal(msg.Value, &token); err != nil {
			slog.Error("persistence unmarshal error", "err", err)
			continue
		}

		w.useCase.AddToken(token)

		if w.useCase.ShouldFlush(persistTokenThreshold) && w.flushing.CompareAndSwap(false, true) {
			go func() {
				defer w.flushing.Store(false)
				w.flush(ctx)
			}()
		}
	}
}

func (w *PersistenceWorker) runFlusher(ctx context.Context) {
	ticker := time.NewTicker(persistFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.flush(context.Background())
			return
		case <-ticker.C:
			if w.flushing.CompareAndSwap(false, true) {
				w.flush(ctx)
				w.flushing.Store(false)
			}
		}
	}
}

func (w *PersistenceWorker) flush(ctx context.Context) {
	if err := w.useCase.Flush(ctx); err != nil {
		slog.Error("persistence flush failed", "err", err)
		return
	}
	slog.Info("persistence flushed")
}
