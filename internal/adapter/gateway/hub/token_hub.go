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
	ch := make(chan usecase.PubSubToken, 1000) // 1000 >> 15 tokens/request, never drops
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
	select {
	case ch <- token:
	default:
		// channel đầy — chỉ xảy ra nếu handler bị block lâu hơn 1000 tokens
	}
}
