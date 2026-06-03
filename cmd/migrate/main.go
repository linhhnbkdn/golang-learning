package main

import (
	"log/slog"
	"os"

	"golang-learning/config"
	gatewaystore "golang-learning/internal/adapter/gateway/store"

	"github.com/joho/godotenv"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	_ = godotenv.Load()
	cfg := config.Load()

	db, err := gorm.Open(gormpg.Open(cfg.DatabaseURL), &gorm.Config{})
	if err != nil {
		slog.Error("connect failed", "err", err)
		os.Exit(1)
	}

	if err := db.AutoMigrate(
		&gatewaystore.SessionModel{},
		&gatewaystore.MessageModel{},
	); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}
	slog.Info("migration completed")
}
