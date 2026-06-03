package postgres

import (
	"context"

	"golang-learning/internal/entity"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MessageStore struct {
	db *gorm.DB
}

func NewMessageStore(db *gorm.DB) *MessageStore {
	return &MessageStore{db: db}
}

func (s *MessageStore) SaveMessage(ctx context.Context, msg entity.Message) error {
	session := SessionModel{SessionID: msg.SessionID}
	if err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&session).Error; err != nil {
		return err
	}
	message := MessageModel{
		SessionID: msg.SessionID,
		RequestID: msg.RequestID,
		Role:      string(msg.Role),
		Content:   msg.Content,
	}
	return s.db.WithContext(ctx).Create(&message).Error
}

func (s *MessageStore) GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error) {
	var rows []MessageModel
	if err := s.db.WithContext(ctx).
		Where("session_id = ?", sessionID).
		Order("id asc").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	messages := make([]entity.Message, len(rows))
	for i, r := range rows {
		messages[i] = entity.Message{
			SessionID: sessionID,
			RequestID: r.RequestID,
			Role:      entity.MessageRole(r.Role),
			Content:   r.Content,
		}
	}
	return messages, nil
}
