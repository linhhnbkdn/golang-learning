package postgres

type MessageModel struct {
	ID        uint   `gorm:"column:id;primaryKey;autoIncrement"`
	SessionID string `gorm:"column:session_id;not null"`
	RequestID string `gorm:"column:request_id;not null"`
	Role      string `gorm:"column:role;not null"`
	Content   string `gorm:"column:content;not null"`
}

func (MessageModel) TableName() string { return "messages" }
