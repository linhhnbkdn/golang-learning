package httppresenter

// SendMessagePresenter formats SendMessageUseCase output for HTTP responses.
type SendMessagePresenter struct {
	RequestID string
	Err       error
}

func (p *SendMessagePresenter) PresentRequestID(requestID string) {
	p.RequestID = requestID
}

func (p *SendMessagePresenter) PresentError(err error) {
	p.Err = err
}
