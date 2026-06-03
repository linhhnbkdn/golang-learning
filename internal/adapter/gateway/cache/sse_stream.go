package cache

import (
	"context"
	"fmt"
	"time"

	"golang-learning/internal/usecase"

	"github.com/redis/go-redis/v9"
)

type SSEStreamImpl struct {
	client *redis.Client
}

func NewSSEStream(client *redis.Client) *SSEStreamImpl {
	return &SSEStreamImpl{client: client}
}

func (s *SSEStreamImpl) key(requestID string) string {
	return fmt.Sprintf("sse:%s", requestID)
}

func (s *SSEStreamImpl) Publish(ctx context.Context, requestID, delta string) error {
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.key(requestID),
		Values: map[string]any{"delta": delta, "done": "0"},
	}).Err()
}

func (s *SSEStreamImpl) PublishDone(ctx context.Context, requestID string) error {
	key := s.key(requestID)
	if err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]any{"delta": "", "done": "1"},
	}).Err(); err != nil {
		return err
	}
	return s.client.Expire(ctx, key, 5*time.Minute).Err()
}

func (s *SSEStreamImpl) Read(ctx context.Context, requestID, lastID string) ([]usecase.SSEToken, error) {
	result, err := s.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{s.key(requestID), lastID},
		Block:   30 * time.Second,
		Count:   100,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var tokens []usecase.SSEToken
	for _, stream := range result {
		for _, msg := range stream.Messages {
			tokens = append(tokens, usecase.SSEToken{
				ID:    msg.ID,
				Delta: fmt.Sprint(msg.Values["delta"]),
				Done:  fmt.Sprint(msg.Values["done"]) == "1",
			})
		}
	}
	return tokens, nil
}
