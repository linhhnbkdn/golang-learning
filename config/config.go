package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers []string
	RedisURL     string
	DatabaseURL  string
	RedisTTL     int
	LLMProvider  string
	OpenAIAPIKey string
	Port         string
}

func Load() Config {
	dbURL := os.Getenv("DATABASE_URL")
	dbURL = strings.Replace(dbURL, "postgresql+asyncpg://", "postgresql://", 1)

	ttl := 86400
	if v := os.Getenv("REDIS_TTL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ttl = n
		}
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	brokers := strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"), ",")

	return Config{
		KafkaBrokers: brokers,
		RedisURL:     os.Getenv("REDIS_URL"),
		DatabaseURL:  dbURL,
		RedisTTL:     ttl,
		LLMProvider:  os.Getenv("LLM_PROVIDER"),
		OpenAIAPIKey: os.Getenv("OPENAI_API_KEY"),
		Port:         port,
	}
}
