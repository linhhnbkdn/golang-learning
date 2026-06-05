package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers    []string
	RedisURL        string
	DatabaseURL     string
	RedisTTL        int
	LLMProvider     string
	OpenAIAPIKey    string
	JWTSecret       string
	Port            string
	CallbackSecret     string
	APIHost            string
	GRPCPort           string
	GRPCAdvertisedAddr string
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

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	grpcAdvertisedAddr := os.Getenv("GRPC_ADVERTISED_ADDR")
	if grpcAdvertisedAddr == "" {
		grpcAdvertisedAddr = "api:" + grpcPort
	}

	brokers := strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"), ",")

	return Config{
		KafkaBrokers:       brokers,
		RedisURL:           os.Getenv("REDIS_URL"),
		DatabaseURL:        dbURL,
		RedisTTL:           ttl,
		LLMProvider:        os.Getenv("LLM_PROVIDER"),
		OpenAIAPIKey:       os.Getenv("OPENAI_API_KEY"),
		JWTSecret:          os.Getenv("JWT_SECRET"),
		Port:               port,
		CallbackSecret:     os.Getenv("CALLBACK_SECRET"),
		APIHost:            os.Getenv("API_HOST"),
		GRPCPort:           grpcPort,
		GRPCAdvertisedAddr: grpcAdvertisedAddr,
	}
}
