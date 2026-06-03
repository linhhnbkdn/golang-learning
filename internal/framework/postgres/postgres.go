package postgres

import (
	"context"
	"strings"

	"golang-learning/config"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func NewPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error { pool.Close(); return nil },
	})
	return pool, nil
}

func ParseAddr(url string) string {
	return strings.TrimPrefix(url, "postgresql://")
}
