package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RequestOwnerStore struct {
	client *redis.Client
}

func NewRequestOwnerStore(client *redis.Client) *RequestOwnerStore {
	return &RequestOwnerStore{client: client}
}

func (s *RequestOwnerStore) SetRequestOwner(ctx context.Context, requestID, userID string) error {
	key := fmt.Sprintf("request_owner:%s", requestID)
	return s.client.Set(ctx, key, userID, 5*time.Minute).Err()
}

func (s *RequestOwnerStore) GetRequestOwner(ctx context.Context, requestID string) (string, error) {
	key := fmt.Sprintf("request_owner:%s", requestID)
	return s.client.Get(ctx, key).Result()
}
