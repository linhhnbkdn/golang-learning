package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RequestOwnerStoreImpl struct {
	client *redis.Client
}

func NewRequestOwnerStore(client *redis.Client) *RequestOwnerStoreImpl {
	return &RequestOwnerStoreImpl{client: client}
}

func (s *RequestOwnerStoreImpl) SetRequestOwner(ctx context.Context, requestID, userID string) error {
	key := fmt.Sprintf("request_owner:%s", requestID)
	return s.client.Set(ctx, key, userID, 5*time.Minute).Err()
}

func (s *RequestOwnerStoreImpl) GetRequestOwner(ctx context.Context, requestID string) (string, error) {
	key := fmt.Sprintf("request_owner:%s", requestID)
	return s.client.Get(ctx, key).Result()
}
