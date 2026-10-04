package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Addr                    string
	DatabaseURL             string
	DatabaseMaxConns        int32
	AuthRequired            bool
	BootstrapKey            string
	RateLimitPerSecond      int
	MaxPendingDeliveries    int64
	MaxConcurrentDeliveries int
	AllowPrivateWebhookURLs bool
	WorkerEnabled           bool
}

func Load() (Config, error) {
	port := getEnv("PORT", "8080")
	maxConns, err := strconv.ParseInt(getEnv("DB_MAX_CONNS", "5"), 10, 32)
	if err != nil || maxConns < 1 {
		return Config{}, fmt.Errorf("DB_MAX_CONNS must be a positive integer")
	}
	authRequired, err := strconv.ParseBool(getEnv("HOOKRELAY_AUTH_REQUIRED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("HOOKRELAY_AUTH_REQUIRED must be true or false")
	}
	rateLimit, err := positiveOrZero("HOOKRELAY_RATE_LIMIT_PER_SECOND", "0")
	if err != nil {
		return Config{}, err
	}
	maxPending, err := strconv.ParseInt(getEnv("HOOKRELAY_MAX_PENDING_DELIVERIES", "10000"), 10, 64)
	if err != nil || maxPending < 0 {
		return Config{}, fmt.Errorf("HOOKRELAY_MAX_PENDING_DELIVERIES must be zero or a positive integer")
	}
	maxConcurrent, err := positiveOrZero("HOOKRELAY_MAX_CONCURRENT_DELIVERIES", "10")
	if err != nil {
		return Config{}, err
	}
	allowPrivate, err := boolEnv("HOOKRELAY_ALLOW_PRIVATE_NETWORKS", false)
	if err != nil {
		return Config{}, err
	}
	workerEnabled, err := boolEnv("HOOKRELAY_WORKER_ENABLED", true)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Addr:                    ":" + port,
		DatabaseURL:             os.Getenv("DATABASE_URL"),
		DatabaseMaxConns:        int32(maxConns),
		AuthRequired:            authRequired,
		BootstrapKey:            os.Getenv("HOOKRELAY_BOOTSTRAP_KEY"),
		RateLimitPerSecond:      rateLimit,
		MaxPendingDeliveries:    maxPending,
		MaxConcurrentDeliveries: maxConcurrent,
		AllowPrivateWebhookURLs: allowPrivate,
		WorkerEnabled:           workerEnabled,
	}, nil
}

func boolEnv(key string, fallback bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", key)
	}
	return parsed, nil
}

func positiveOrZero(key, fallback string) (int, error) {
	value, err := strconv.Atoi(getEnv(key, fallback))
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be zero or a positive integer", key)
	}
	return value, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
