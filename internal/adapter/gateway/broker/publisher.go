package broker

import (
	"context"
	"encoding/json"
	"time"

	"golang-learning/config"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topicRequests  = "chat.requests"
	topicResponses = "chat.responses"
	topicCompleted = "chat.completed"
)

type EventPublisherImpl struct {
	writer *kafka.Writer
}

func NewEventPublisher(cfg config.Config) *EventPublisherImpl {
	return &EventPublisherImpl{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			BatchSize:    500,
			Async:        true,
			Compression:  kafka.Lz4, // giảm I/O ~50-60%, CPU trade-off nhỏ
		},
	}
}

func (p *EventPublisherImpl) PublishRequest(ctx context.Context, req shared.ChatRequest) error {
	return p.write(ctx, topicRequests, req)
}

func (p *EventPublisherImpl) PublishResponse(ctx context.Context, resp shared.ChatResponse) error {
	return p.write(ctx, topicResponses, resp)
}

func (p *EventPublisherImpl) PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error {
	return p.write(ctx, topicCompleted, completed)
}

func (p *EventPublisherImpl) Flush() {}

func (p *EventPublisherImpl) Close() error {
	return p.writer.Close()
}

func (p *EventPublisherImpl) write(ctx context.Context, topic string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: data,
	})
}
