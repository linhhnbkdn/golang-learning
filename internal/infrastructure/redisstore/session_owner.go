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

// ClaimOwner uses SetNX (atomic set-if-not-exists).
// Returns true if this user owns the session (just claimed or already theirs).
// Returns false if the session is owned by a different user.
func (s *SessionOwnerStore) ClaimOwner(ctx context.Context, sessionID, userID string) (bool, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)

	// Atomic: only sets if key does not exist
	claimed, err := s.client.SetNX(ctx, key, userID, s.ttl).Result()
	if err != nil {
		return false, err
	}
	if claimed {
		return true, nil // first to claim
	}

	// Key already exists — check if this user is the existing owner
	existing, err := s.client.Get(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return existing == userID, nil
}

func (s *SessionOwnerStore) GetOwner(ctx context.Context, sessionID string) (string, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)
	return s.client.Get(ctx, key).Result()
}
