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
	topicRequests    = "chat.requests"
	topicCompleted   = "chat.completed"
	topicStreamFE    = "stream-llm-fe"
	topicPersistLLM  = "persistence-llm"
)

type EventPublisherImpl struct {
	writer        *kafka.Writer
	streamWriter  *kafka.Writer
	persistWriter *kafka.Writer
}

func NewEventPublisher(cfg config.Config) *EventPublisherImpl {
	addr := kafka.TCP(cfg.KafkaBrokers...)
	return &EventPublisherImpl{
		writer: &kafka.Writer{
			Addr:         addr,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 10 * time.Millisecond,
			BatchSize:    500,
			Async:        true,
			Compression:  kafka.Lz4,
		},
		streamWriter: &kafka.Writer{
			Addr:         addr,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 0,
			BatchSize:    1,
			Async:        true,
		},
		persistWriter: &kafka.Writer{
			Addr:         addr,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 100 * time.Millisecond,
			BatchSize:    500,
			Async:        true,
			Compression:  kafka.Lz4,
		},
	}
}

func (p *EventPublisherImpl) PublishRequest(ctx context.Context, req shared.ChatRequest) error {
	return p.write(ctx, p.writer, topicRequests, req.SessionID, req)
}

func (p *EventPublisherImpl) PublishCompleted(ctx context.Context, completed shared.ChatCompleted) error {
	return p.write(ctx, p.writer, topicCompleted, completed.SessionID, completed)
}

func (p *EventPublisherImpl) PublishToken(ctx context.Context, token shared.TokenEvent) error {
	data, err := json.Marshal(token)
	if err != nil {
		return err
	}
	key := []byte(token.SessionID)
	msg := kafka.Message{Key: key, Value: data}

	streamMsg := msg
	streamMsg.Topic = topicStreamFE
	persistMsg := msg
	persistMsg.Topic = topicPersistLLM

	if err := p.streamWriter.WriteMessages(ctx, streamMsg); err != nil {
		return err
	}
	return p.persistWriter.WriteMessages(ctx, persistMsg)
}

func (p *EventPublisherImpl) Close() error {
	_ = p.streamWriter.Close()
	_ = p.persistWriter.Close()
	return p.writer.Close()
}

func (p *EventPublisherImpl) write(ctx context.Context, w *kafka.Writer, topic, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	})
}
