package postgres

import (
	"context"

	"golang-learning/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MessageStore struct {
	pool *pgxpool.Pool
}

func NewMessageStore(pool *pgxpool.Pool) *MessageStore {
	return &MessageStore{pool: pool}
}

func (s *MessageStore) SaveMessage(ctx context.Context, msg domain.Message) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO sessions (session_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		msg.SessionID,
	)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO messages (session_id, request_id, role, content) VALUES ($1, $2, $3, $4)`,
		msg.SessionID, msg.RequestID, string(msg.Role), msg.Content,
	)
	return err
}

func (s *MessageStore) GetHistory(ctx context.Context, sessionID string) ([]domain.Message, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT request_id, role, content FROM messages WHERE session_id = $1 ORDER BY id ASC`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []domain.Message
	for rows.Next() {
		var requestID, role, content string
		if err := rows.Scan(&requestID, &role, &content); err != nil {
			return nil, err
		}
		messages = append(messages, domain.Message{
			SessionID: sessionID,
			RequestID: requestID,
			Role:      domain.MessageRole(role),
			Content:   content,
		})
	}
	return messages, rows.Err()
}
