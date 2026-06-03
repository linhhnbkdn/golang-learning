package state

import (
	"sync"

	"golang-learning/shared"
)

type SSEState struct {
	queues sync.Map // map[requestID]chan shared.ChatResponse
}

// Register returns a channel and true if successfully registered.
// Returns nil, false if requestID is already registered (prevents hijacking).
func (s *SSEState) Register(requestID string) (chan shared.ChatResponse, bool) {
	ch := make(chan shared.ChatResponse, 100)
	_, loaded := s.queues.LoadOrStore(requestID, ch)
	if loaded {
		return nil, false
	}
	return ch, true
}

func (s *SSEState) Route(resp shared.ChatResponse) {
	v, ok := s.queues.Load(resp.RequestID)
	if !ok {
		return
	}
	ch := v.(chan shared.ChatResponse)
	select {
	case ch <- resp:
	default:
	}
}

func (s *SSEState) Unregister(requestID string) {
	if v, ok := s.queues.LoadAndDelete(requestID); ok {
		close(v.(chan shared.ChatResponse))
	}
}
