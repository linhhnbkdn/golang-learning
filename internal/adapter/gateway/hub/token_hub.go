package hub

import (
	"sync"

	"golang-learning/internal/usecase"
)

type TokenHub struct {
	pending sync.Map // requestID → chan usecase.PubSubToken
}

func New() *TokenHub {
	return &TokenHub{}
}

func (h *TokenHub) Register(requestID string) (<-chan usecase.PubSubToken, func()) {
	ch := make(chan usecase.PubSubToken, 100)
	h.pending.Store(requestID, ch)
	cleanup := func() {
		h.pending.Delete(requestID)
	}
	return ch, cleanup
}

func (h *TokenHub) Deliver(requestID string, token usecase.PubSubToken) {
	val, ok := h.pending.Load(requestID)
	if !ok {
		return
	}
	ch := val.(chan usecase.PubSubToken)
	if token.Done {
		// done token không được drop — block nếu cần
		ch <- token
		return
	}
	select {
	case ch <- token:
	default:
		// delta token drop khi channel đầy — acceptable
	}
}
