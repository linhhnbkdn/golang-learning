package httppresenter

import (
	"golang-learning/internal/entity"
)

// MessageView is the HTTP response shape for a single message.
type MessageView struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	RequestID string `json:"request_id"`
}

// GetHistoryPresenter formats GetHistoryUseCase output for HTTP responses.
type GetHistoryPresenter struct {
	Messages []MessageView
	Err      error
}

func (p *GetHistoryPresenter) PresentMessages(messages []entity.Message) {
	p.Messages = make([]MessageView, len(messages))
	for i, m := range messages {
		p.Messages[i] = MessageView{
			Role:      string(m.Role),
			Content:   m.Content,
			RequestID: m.RequestID,
		}
	}
}

func (p *GetHistoryPresenter) PresentError(err error) {
	p.Err = err
}
