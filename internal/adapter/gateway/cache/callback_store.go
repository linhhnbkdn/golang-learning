package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const callbackTTL = 5 * time.Minute

type CallbackStoreImpl struct {
	client *redis.Client
}

func NewCallbackStore(client *redis.Client) *CallbackStoreImpl {
	return &CallbackStoreImpl{client: client}
}

func (s *CallbackStoreImpl) SetCallback(ctx context.Context, requestID, grpcAddr string) error {
	return s.client.Set(ctx, callbackKey(requestID), grpcAddr, callbackTTL).Err()
}

func (s *CallbackStoreImpl) GetCallback(ctx context.Context, requestID string) (string, error) {
	return s.client.Get(ctx, callbackKey(requestID)).Result()
}

func callbackKey(requestID string) string {
	return "stream:callback:" + requestID
}
