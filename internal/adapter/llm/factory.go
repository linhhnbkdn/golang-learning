package llm

import (
	"fmt"

	"golang-learning/internal/usecase"
)

func NewTokenGenerator(provider string) (usecase.TokenGenerator, error) {
	switch provider {
	case "mock", "":
		return &MockLLMStrategy{}, nil
	default:
		return nil, fmt.Errorf("unknown LLM provider: %s", provider)
	}
}
