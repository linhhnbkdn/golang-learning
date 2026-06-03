package postgres

type SessionModel struct {
	SessionID string `gorm:"column:session_id;primaryKey"`
}

func (SessionModel) TableName() string { return "sessions" }
