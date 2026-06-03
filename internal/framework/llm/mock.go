package llm

import (
	"context"
	"math/rand"
	"strings"
	"time"
)

var responses = []string{
	"Xin chào! Tôi là một AI trợ lý. Tôi có thể giúp gì cho bạn?",
	"Đây là một hệ thống event-driven streaming sử dụng Kafka và Go.",
	"Latency là thời gian để một packet đi từ sender đến receiver.",
	"Throughput là lượng data thực sự được truyền thành công mỗi giây.",
	"Redis sorted sets rất phù hợp để lưu conversation history theo thứ tự thời gian.",
}

type MockLLMStrategy struct{}

func (m *MockLLMStrategy) Generate(ctx context.Context, content string) (<-chan string, error) {
	ch := make(chan string)
	go func() {
		defer close(ch)
		response := responses[rand.Intn(len(responses))]
		words := strings.Fields(response)
		for i, word := range words {
			token := word
			if i > 0 {
				token = " " + word
			}
			select {
			case ch <- token:
			case <-ctx.Done():
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return ch, nil
}
