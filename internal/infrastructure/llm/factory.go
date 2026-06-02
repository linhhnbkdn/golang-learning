package llm

import (
	"fmt"

	"golang-learning/internal/application/port"
)

func NewTokenGenerator(provider string) (port.TokenGenerator, error) {
	switch provider {
	case "mock", "":
		return &MockLLMStrategy{}, nil
	default:
		return nil, fmt.Errorf("unknown LLM provider: %s", provider)
	}
}
