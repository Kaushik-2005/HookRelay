package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Addr             string
	DatabaseURL      string
	DatabaseMaxConns int32
}

func Load() (Config, error) {
	port := getEnv("PORT", "8080")
	maxConns, err := strconv.ParseInt(getEnv("DB_MAX_CONNS", "5"), 10, 32)
	if err != nil || maxConns < 1 {
		return Config{}, fmt.Errorf("DB_MAX_CONNS must be a positive integer")
	}

	return Config{
		Addr:             ":" + port,
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		DatabaseMaxConns: int32(maxConns),
	}, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
