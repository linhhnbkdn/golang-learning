package postgres

import (
	"context"

	"golang-learning/config"

	"go.uber.org/fx"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewDB(lc fx.Lifecycle, cfg config.Config) (*gorm.DB, error) {
	db, err := gorm.Open(gormpg.Open(cfg.DatabaseURL), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error { return sqlDB.Close() },
	})
	return db, nil
}
