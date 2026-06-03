package store

import "golang-learning/internal/entity"

type MessageModel struct {
	ID        uint         `gorm:"column:id;primaryKey;autoIncrement"`
	SessionID string       `gorm:"column:session_id;not null"`
	RequestID string       `gorm:"column:request_id;not null"`
	Role      string       `gorm:"column:role;not null"`
	Content   string       `gorm:"column:content;not null"`
	Session   SessionModel `gorm:"foreignKey:SessionID;references:SessionID"`
}

func (MessageModel) TableName() string { return "messages" }

func (m MessageModel) ToEntity() entity.Message {
	return entity.Message{
		SessionID: m.SessionID,
		RequestID: m.RequestID,
		Role:      entity.MessageRole(m.Role),
		Content:   m.Content,
	}
}

func messageFromEntity(msg entity.Message) MessageModel {
	return MessageModel{
		SessionID: msg.SessionID,
		RequestID: msg.RequestID,
		Role:      string(msg.Role),
		Content:   msg.Content,
	}
}
