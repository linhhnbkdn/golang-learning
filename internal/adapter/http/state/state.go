package state

import (
	"sync"

	"golang-learning/shared"
)

type SSEState struct {
	queues sync.Map // map[requestID]chan shared.ChatResponse
}

func (s *SSEState) Register(requestID string) chan shared.ChatResponse {
	ch := make(chan shared.ChatResponse, 100)
	s.queues.Store(requestID, ch)
	return ch
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
