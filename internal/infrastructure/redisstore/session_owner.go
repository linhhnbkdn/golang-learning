package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type SessionOwnerStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewSessionOwnerStore(client *redis.Client) *SessionOwnerStore {
	return &SessionOwnerStore{
		client: client,
		ttl:    24 * time.Hour,
	}
}

func (s *SessionOwnerStore) SetOwner(ctx context.Context, sessionID, userID string) error {
	key := fmt.Sprintf("session_owner:%s", sessionID)
	return s.client.Set(ctx, key, userID, s.ttl).Err()
}

func (s *SessionOwnerStore) GetOwner(ctx context.Context, sessionID string) (string, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)
	return s.client.Get(ctx, key).Result()
}
