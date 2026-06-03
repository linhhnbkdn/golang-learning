package event

import (
	"context"
	"encoding/json"

	"golang-learning/config"
	"golang-learning/shared"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topicRequests  = "chat.requests"
	topicResponses = "chat.responses"
	topicCompleted = "chat.completed"
)

type EventPublisher struct {
	writer *kafka.Writer
}

func NewEventPublisher(cfg config.Config) *EventPublisher {
	return &EventPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.KafkaBrokers...),
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *EventPublisher) PublishRequest(ctx context.Context, req shared.ChatRequest) error {
	return p.write(ctx, topicRequests, req)
}

func (p *EventPublisher) PublishResponse(ctx context.Context, resp shared.ChatResponse) error {
	return p.write(ctx, topicResponses, resp)
}

func (p *EventPublisher) PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error {
	return p.write(ctx, topicCompleted, completed)
}

func (p *EventPublisher) Flush() {}

func (p *EventPublisher) Close() error {
	return p.writer.Close()
}

func (p *EventPublisher) write(ctx context.Context, topic string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: data,
	})
}
