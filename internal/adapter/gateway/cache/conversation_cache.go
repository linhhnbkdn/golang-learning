package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"golang-learning/config"
	"golang-learning/internal/entity"

	"github.com/redis/go-redis/v9"
)

type ConversationCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewConversationCache(client *redis.Client, cfg config.Config) *ConversationCache {
	return &ConversationCache{
		client: client,
		ttl:    time.Duration(cfg.RedisTTL) * time.Second,
	}
}

type messageRecord struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	RequestID string `json:"request_id"`
}

func (c *ConversationCache) SaveMessage(ctx context.Context, msg entity.Message) error {
	key := fmt.Sprintf("conversation:%s", msg.SessionID)
	rec := messageRecord{
		Role:      string(msg.Role),
		Content:   msg.Content,
		RequestID: msg.RequestID,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	score := float64(time.Now().UnixNano()) / 1e9
	if err := c.client.ZAdd(ctx, key, redis.Z{Score: score, Member: string(data)}).Err(); err != nil {
		return err
	}
	return c.client.Expire(ctx, key, c.ttl).Err()
}

func (c *ConversationCache) GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error) {
	key := fmt.Sprintf("conversation:%s", sessionID)
	raw, err := c.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	messages := make([]entity.Message, 0, len(raw))
	for _, r := range raw {
		var rec messageRecord
		if err := json.Unmarshal([]byte(r), &rec); err != nil {
			continue
		}
		messages = append(messages, entity.Message{
			SessionID: sessionID,
			RequestID: rec.RequestID,
			Role:      entity.MessageRole(rec.Role),
			Content:   rec.Content,
		})
	}
	return messages, nil
}
