package httppresenter

import (
	"golang-learning/internal/entity"
)

// MessageDTO is the HTTP response shape for a single message.
type MessageDTO struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	RequestID string `json:"request_id"`
}

// GetHistoryPresenter formats GetHistoryUseCase output for HTTP responses.
type GetHistoryPresenter struct {
	Messages []MessageDTO
	Err      error
}

func (p *GetHistoryPresenter) PresentMessages(messages []entity.Message) {
	p.Messages = make([]MessageDTO, len(messages))
	for i, m := range messages {
		p.Messages[i] = MessageDTO{
			Role:      string(m.Role),
			Content:   m.Content,
			RequestID: m.RequestID,
		}
	}
}

func (p *GetHistoryPresenter) PresentError(err error) {
	p.Err = err
}
