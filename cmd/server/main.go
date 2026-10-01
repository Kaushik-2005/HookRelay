package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/config"
	"hookrelay/internal/db"
	"hookrelay/internal/deliveries"
	"hookrelay/internal/events"
	"hookrelay/internal/projects"
	"hookrelay/internal/webhooks"
	"hookrelay/internal/worker"
)

type healthResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Printf("configuration error: %v", err)
		os.Exit(1)
	}

	var pool *pgxpool.Pool
	if cfg.DatabaseURL != "" {
		poolConfig, parseErr := pgxpool.ParseConfig(cfg.DatabaseURL)
		if parseErr != nil {
			log.Printf("database configuration error: %v", parseErr)
			os.Exit(1)
		}
		poolConfig.MaxConns = cfg.DatabaseMaxConns
		pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err != nil {
			log.Printf("database configuration error: %v", err)
			os.Exit(1)
		}
		defer pool.Close()

		if err := db.Migrate(context.Background(), pool, "migrations"); err != nil {
			log.Printf("database migration error: %v", err)
			os.Exit(1)
		}
		go worker.New(pool, log.Default()).Run(context.Background())
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(pool))
	projectHandler := projects.NewHandler(pool)
	mux.HandleFunc("POST /projects", projectHandler.Create)
	mux.HandleFunc("GET /projects", projectHandler.List)
	mux.HandleFunc("GET /projects/{project_id}", projectHandler.Get)
	webhookHandler := webhooks.NewHandler(pool)
	mux.HandleFunc("POST /projects/{project_id}/webhooks", webhookHandler.Create)
	mux.HandleFunc("GET /projects/{project_id}/webhooks", webhookHandler.List)
	mux.HandleFunc("PATCH /webhooks/{webhook_id}", webhookHandler.Update)
	mux.HandleFunc("DELETE /webhooks/{webhook_id}", webhookHandler.Delete)
	eventHandler := events.NewHandler(pool)
	mux.HandleFunc("POST /projects/{project_id}/events", eventHandler.Create)
	mux.HandleFunc("GET /projects/{project_id}/events", eventHandler.List)
	mux.HandleFunc("GET /events/{event_id}", eventHandler.Get)
	deliveryHandler := deliveries.NewHandler(pool)
	mux.HandleFunc("GET /events/{event_id}/deliveries", deliveryHandler.ByEvent)
	mux.HandleFunc("GET /webhooks/{webhook_id}/deliveries", deliveryHandler.ByWebhook)
	mux.HandleFunc("GET /deliveries/{delivery_id}", deliveryHandler.Get)
	mux.HandleFunc("POST /deliveries/{delivery_id}/retry", deliveryHandler.Retry)

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("HookRelay listening on %s", cfg.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func healthHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		databaseStatus := "not_configured"
		statusCode := http.StatusOK

		if pool != nil {
			databaseStatus = "connected"
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := pool.Ping(ctx); err != nil {
				databaseStatus = "unavailable"
				statusCode = http.StatusServiceUnavailable
			}
		}

		status := "degraded"
		if statusCode == http.StatusOK {
			status = "ok"
		}
		writeJSON(w, statusCode, healthResponse{Status: status, Database: databaseStatus})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("method=%s path=%s duration=%s", r.Method, r.URL.Path, time.Since(started))
	})
}
