package state

import (
	"sync"

	"golang-learning/shared"
)

type SSEState struct {
	mu      sync.Mutex
	queues  map[string]chan shared.ChatResponse
	pending map[string][]shared.ChatResponse
}

func NewSSEState() *SSEState {
	return &SSEState{
		queues:  make(map[string]chan shared.ChatResponse),
		pending: make(map[string][]shared.ChatResponse),
	}
}

// Register returns a channel for the requestID.
// Drains any tokens buffered before client connected.
// Returns nil, false if already registered (prevents hijacking).
func (s *SSEState) Register(requestID string) (chan shared.ChatResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[requestID]; exists {
		return nil, false
	}
	ch := make(chan shared.ChatResponse, 100)
	s.queues[requestID] = ch

	for _, msg := range s.pending[requestID] {
		ch <- msg
	}
	delete(s.pending, requestID)

	return ch, true
}

// Route sends a token to the client channel.
// If client has not connected yet, buffers the token.
func (s *SSEState) Route(resp shared.ChatResponse) {
	s.mu.Lock()
	ch, ok := s.queues[resp.RequestID]
	if !ok {
		s.pending[resp.RequestID] = append(s.pending[resp.RequestID], resp)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	select {
	case ch <- resp:
	default:
	}
}

func (s *SSEState) Unregister(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ch, ok := s.queues[requestID]; ok {
		close(ch)
		delete(s.queues, requestID)
	}
	delete(s.pending, requestID)
}
