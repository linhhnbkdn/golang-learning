package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type SessionOwnerStoreImpl struct {
	client *redis.Client
	ttl    time.Duration
}

func NewSessionOwnerStore(client *redis.Client) *SessionOwnerStoreImpl {
	return &SessionOwnerStoreImpl{
		client: client,
		ttl:    24 * time.Hour,
	}
}

// ClaimOwner uses SetNX (atomic set-if-not-exists).
// Returns true if this user owns the session (just claimed or already theirs).
// Returns false if the session is owned by a different user.
func (s *SessionOwnerStoreImpl) ClaimOwner(ctx context.Context, sessionID, userID string) (bool, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)

	claimed, err := s.client.SetNX(ctx, key, userID, s.ttl).Result()
	if err != nil {
		return false, err
	}
	if claimed {
		return true, nil
	}

	existing, err := s.client.Get(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return existing == userID, nil
}

func (s *SessionOwnerStoreImpl) GetOwner(ctx context.Context, sessionID string) (string, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)
	return s.client.Get(ctx, key).Result()
}
