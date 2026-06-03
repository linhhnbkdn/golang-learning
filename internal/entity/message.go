package entity

type MessageRole string

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
)

type Message struct {
	SessionID string
	RequestID string
	Role      MessageRole
	Content   string
}
