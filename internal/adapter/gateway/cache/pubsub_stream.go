package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"golang-learning/internal/usecase"

	"github.com/redis/go-redis/v9"
)

type sessionSub struct {
	mu      sync.Mutex
	current chan<- usecase.PubSubToken
	dead    chan struct{} // closed when dispatch goroutine exits (Redis disconnect)
}

// PubSubStreamImpl maintains one Redis subscription per session rather than
// one per request, eliminating the ps.Receive() round-trip on every chat turn.
type PubSubStreamImpl struct {
	client   *redis.Client
	mu       sync.Mutex
	sessions map[string]*sessionSub
}

func NewPubSubStream(client *redis.Client) *PubSubStreamImpl {
	return &PubSubStreamImpl{
		client:   client,
		sessions: make(map[string]*sessionSub),
	}
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
	ss, err := s.getOrCreate(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan usecase.PubSubToken, 100)

	ss.mu.Lock()
	ss.current = ch
	ss.mu.Unlock()

	cleanup := func() {
		ss.mu.Lock()
		if ss.current == (chan<- usecase.PubSubToken)(ch) {
			ss.current = nil
		}
		ss.mu.Unlock()
	}

	return ch, cleanup, nil
}

// getOrCreate returns a live session subscription, creating one if needed.
// Stale sessions (whose dispatch goroutine has exited) are replaced transparently.
func (s *PubSubStreamImpl) getOrCreate(ctx context.Context, sessionID string) (*sessionSub, error) {
	s.mu.Lock()
	if ss, ok := s.sessions[sessionID]; ok {
		select {
		case <-ss.dead:
			// dispatch goroutine exited (Redis disconnect) — fall through to recreate
			delete(s.sessions, sessionID)
		default:
			s.mu.Unlock()
			return ss, nil
		}
	}

	ss := &sessionSub{dead: make(chan struct{})}
	s.sessions[sessionID] = ss
	s.mu.Unlock()

	ps := s.client.Subscribe(ctx, s.key(sessionID))
	if _, err := ps.Receive(ctx); err != nil {
		ps.Close()
		s.mu.Lock()
		delete(s.sessions, sessionID)
		s.mu.Unlock()
		return nil, err
	}

	go func() {
		defer func() {
			close(ss.dead)
			s.mu.Lock()
			delete(s.sessions, sessionID)
			s.mu.Unlock()
		}()
		for msg := range ps.Channel() {
			var data map[string]string
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				continue
			}
			token := usecase.PubSubToken{
				RequestID: data["request_id"],
				Delta:     data["delta"],
				Done:      data["done"] == "1",
			}
			ss.mu.Lock()
			ch := ss.current
			ss.mu.Unlock()
			if ch != nil {
				ch <- token
			}
		}
	}()

	return ss, nil
}
