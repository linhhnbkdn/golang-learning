package store

import (
	"context"

	"golang-learning/internal/entity"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MessageStoreImpl struct {
	db *gorm.DB
}

func NewMessageStore(db *gorm.DB) *MessageStoreImpl {
	return &MessageStoreImpl{db: db}
}

func (s *MessageStoreImpl) SaveMessage(ctx context.Context, msg entity.Message) error {
	session := SessionModel{SessionID: msg.SessionID}
	if err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Messages").
		Create(&session).Error; err != nil {
		return err
	}
	row := messageFromEntity(msg)
	return s.db.WithContext(ctx).Omit("Session").Create(&row).Error
}

func (s *MessageStoreImpl) BulkSaveMessages(ctx context.Context, msgs []entity.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	for _, m := range msgs {
		seen[m.SessionID] = struct{}{}
	}
	sessions := make([]SessionModel, 0, len(seen))
	for id := range seen {
		sessions = append(sessions, SessionModel{SessionID: id})
	}
	if err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Omit("Messages").
		Create(&sessions).Error; err != nil {
		return err
	}

	rows := make([]MessageModel, len(msgs))
	for i, m := range msgs {
		rows[i] = messageFromEntity(m)
	}
	return s.db.WithContext(ctx).Omit("Session").Create(&rows).Error
}

func (s *MessageStoreImpl) GetHistory(ctx context.Context, sessionID string) ([]entity.Message, error) {
	var rows []MessageModel
	if err := s.db.WithContext(ctx).
		Where("session_id = ?", sessionID).
		Order("id asc").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	messages := make([]entity.Message, len(rows))
	for i, r := range rows {
		messages[i] = r.ToEntity()
	}
	return messages, nil
}
