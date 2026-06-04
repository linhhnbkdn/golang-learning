package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// claimOwnerScript atomically claims or verifies session ownership in one RTT.
// Returns 1 if the caller owns the session (claimed or already theirs), 0 otherwise.
var claimOwnerScript = redis.NewScript(`
local key = KEYS[1]
local user = ARGV[1]
local ttl  = ARGV[2]
local existing = redis.call('GET', key)
if existing == false then
    redis.call('SET', key, user, 'EX', ttl)
    return 1
end
if existing == user then return 1 end
return 0
`)

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

func (s *SessionOwnerStoreImpl) ClaimOwner(ctx context.Context, sessionID, userID string) (bool, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)
	ttlSecs := int64(s.ttl.Seconds())
	result, err := claimOwnerScript.Run(ctx, s.client, []string{key}, userID, ttlSecs).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *SessionOwnerStoreImpl) GetOwner(ctx context.Context, sessionID string) (string, error) {
	key := fmt.Sprintf("session_owner:%s", sessionID)
	return s.client.Get(ctx, key).Result()
}
