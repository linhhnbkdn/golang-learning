package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime"

	"golang-learning/config"
	"golang-learning/internal/usecase"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

type Worker struct {
	useCase     *usecase.ProcessChatRequestUseCase
	reader      *kafka.Reader
	concurrency int
}

func NewWorker(cfg config.Config, useCase *usecase.ProcessChatRequestUseCase) *Worker {
	return &Worker{
		useCase:     useCase,
		concurrency: runtime.NumCPU() * 4,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.KafkaBrokers,
			GroupID:  "llm-worker",
			Topic:    "chat.requests",
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

func (w *Worker) Run(ctx context.Context) error {
	defer w.reader.Close()
	slog.Info("worker started", "concurrency", w.concurrency)

	sem := make(chan struct{}, w.concurrency)

	for {
		msg, err := w.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("worker read error", "err", err)
			continue
		}

		var req shared.ChatRequest
		if err := json.Unmarshal(msg.Value, &req); err != nil {
			slog.Error("worker unmarshal error", "err", err)
			continue
		}

		sem <- struct{}{}
		go func(req shared.ChatRequest) {
			defer func() { <-sem }()
			slog.Info("processing request", "request_id", req.RequestID)
			if err := w.useCase.Execute(ctx, req); err != nil {
				slog.Error("process chat request failed", "err", err, "request_id", req.RequestID)
			}
		}(req)
	}
}
