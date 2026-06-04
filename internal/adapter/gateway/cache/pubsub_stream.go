package cache

import (
	"context"
	"encoding/json"
	"fmt"

	"golang-learning/internal/usecase"

	"github.com/redis/go-redis/v9"
)

type PubSubStreamImpl struct {
	client *redis.Client
}

func NewPubSubStream(client *redis.Client) *PubSubStreamImpl {
	return &PubSubStreamImpl{client: client}
}

func (s *PubSubStreamImpl) key(sessionID string) string {
	return fmt.Sprintf("pubsub:session:%s", sessionID)
}

func (s *PubSubStreamImpl) Publish(ctx context.Context, sessionID, requestID, delta string, done bool) error {
	doneVal := "0"
	if done {
		doneVal = "1"
	}
	payload, err := json.Marshal(map[string]string{
		"request_id": requestID,
		"delta":      delta,
		"done":       doneVal,
	})
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, s.key(sessionID), payload).Err()
}

func (s *PubSubStreamImpl) Subscribe(ctx context.Context, sessionID string) (<-chan usecase.PubSubToken, func(), error) {
	ps := s.client.Subscribe(ctx, s.key(sessionID))
	if _, err := ps.Receive(ctx); err != nil {
		ps.Close()
		return nil, nil, err
	}

	ch := make(chan usecase.PubSubToken, 100)
	go func() {
		defer close(ch)
		msgs := ps.Channel()
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var data map[string]string
				if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
					continue
				}
				token := usecase.PubSubToken{
					RequestID: data["request_id"],
					Delta:     data["delta"],
					Done:      data["done"] == "1",
				}
				ch <- token
				if token.Done {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	unsubscribe := func() { ps.Close() }
	return ch, unsubscribe, nil
}
