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

type streamToken struct {
	RequestID string `json:"request_id"`
	Delta     string `json:"delta"`
	Done      bool   `json:"done"`
}

type pendingToken struct {
	msg   kafka.Message
	token streamToken
}

type StreamingWorker struct {
	useCase *usecase.StreamTokensUseCase
	reader  *kafka.Reader

	mu       sync.Mutex
	channels map[string]chan pendingToken
}

func NewStreamingWorker(cfg config.Config, useCase *usecase.StreamTokensUseCase) *StreamingWorker {
	return &StreamingWorker{
		useCase:  useCase,
		channels: make(map[string]chan pendingToken),
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

		var token streamToken
		if err := json.Unmarshal(msg.Value, &token); err != nil {
			slog.Error("streaming worker unmarshal error", "err", err)
			// commit để skip message lỗi, không block progress
			_ = w.reader.CommitMessages(ctx, msg)
			continue
		}

		w.route(ctx, pendingToken{msg: msg, token: token})
	}
}

func (w *StreamingWorker) route(ctx context.Context, pt pendingToken) {
	w.mu.Lock()
	ch, exists := w.channels[pt.token.RequestID]
	if !exists {
		ch = make(chan pendingToken, 32)
		w.channels[pt.token.RequestID] = ch
		go w.processRequest(ctx, pt.token.RequestID, ch)
	}
	w.mu.Unlock()

	select {
	case ch <- pt:
	default:
		// channel đầy — offload sang goroutine riêng, consumer loop không block
		go func() {
			select {
			case ch <- pt:
			case <-ctx.Done():
				_ = w.reader.CommitMessages(context.Background(), pt.msg)
			}
		}()
	}
}

func (w *StreamingWorker) processRequest(ctx context.Context, requestID string, ch chan pendingToken) {
	defer func() {
		w.mu.Lock()
		delete(w.channels, requestID)
		w.mu.Unlock()
	}()

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	for {
		select {
		case pt := <-ch:
			err := w.useCase.Execute(ctx, shared.TokenEvent{
				RequestID: pt.token.RequestID,
				Delta:     pt.token.Delta,
				Done:      pt.token.Done,
			})
			if err != nil {
				slog.Error("streaming worker deliver error", "err", err, "request_id", requestID)
			}
			// commit sau khi deliver (thành công hay thất bại đều commit để không block progress)
			if cerr := w.reader.CommitMessages(ctx, pt.msg); cerr != nil {
				slog.Error("streaming worker commit error", "err", cerr)
			}
			if pt.token.Done {
				slog.Info("streaming worker request done", "request_id", requestID)
				return
			}
			timer.Reset(30 * time.Second)
		case <-timer.C:
			slog.Warn("streaming worker request timeout", "request_id", requestID)
			return
		case <-ctx.Done():
			return
		}
	}
}
