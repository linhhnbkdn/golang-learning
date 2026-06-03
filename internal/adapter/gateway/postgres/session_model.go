package postgres

type SessionModel struct {
	SessionID string         `gorm:"column:session_id;primaryKey"`
	Messages  []MessageModel `gorm:"foreignKey:SessionID;references:SessionID"`
}

func (SessionModel) TableName() string { return "sessions" }
