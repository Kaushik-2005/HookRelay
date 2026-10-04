package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/config"
	"hookrelay/internal/db"
	"hookrelay/internal/limits"
	"hookrelay/internal/observability"
	"hookrelay/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	if cfg.DatabaseURL == "" {
		log.Fatal("DATABASE_URL is required for the worker")
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database configuration error: %v", err)
	}
	poolConfig.MaxConns = cfg.DatabaseMaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("database configuration error: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(context.Background(), pool, "migrations"); err != nil {
		log.Fatalf("database migration error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	worker.NewWithParallelism(pool, log.Default(), limits.NewWebhookLimiter(cfg.MaxConcurrentDeliveries), observability.NewMetrics(), cfg.AllowPrivateWebhookURLs, cfg.MaxConcurrentDeliveries).Run(ctx)
}
