package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/auth"
	"hookrelay/internal/config"
	"hookrelay/internal/db"
	"hookrelay/internal/deliveries"
	"hookrelay/internal/events"
	"hookrelay/internal/eventtypes"
	"hookrelay/internal/limits"
	"hookrelay/internal/observability"
	"hookrelay/internal/portal"
	"hookrelay/internal/projects"
	"hookrelay/internal/webhooks"
	"hookrelay/internal/worker"
)

type healthResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
}

func main() {
	metrics := observability.NewMetrics()
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
	}
	if cfg.AuthRequired && cfg.BootstrapKey == "" {
		log.Printf("configuration error: HOOKRELAY_BOOTSTRAP_KEY is required when HOOKRELAY_AUTH_REQUIRED=true")
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(pool))
	mux.Handle("GET /metrics", metrics.Handler())
	mux.Handle("GET /portal", portal.Handler())
	mux.Handle("GET /portal/", portal.Handler())
	projectHandler := projects.NewHandler(pool)
	mux.HandleFunc("POST /projects", projectHandler.Create)
	mux.HandleFunc("GET /projects", projectHandler.List)
	mux.HandleFunc("GET /projects/{project_id}", projectHandler.Get)
	webhookHandler := webhooks.NewHandlerWithOptions(pool, cfg.AllowPrivateWebhookURLs)
	mux.HandleFunc("POST /projects/{project_id}/webhooks", webhookHandler.Create)
	mux.HandleFunc("GET /projects/{project_id}/webhooks", webhookHandler.List)
	mux.HandleFunc("PATCH /webhooks/{webhook_id}", webhookHandler.Update)
	mux.HandleFunc("DELETE /webhooks/{webhook_id}", webhookHandler.Delete)
	mux.HandleFunc("POST /webhooks/{webhook_id}/rotate-secret", webhookHandler.RotateSecret)
	mux.HandleFunc("POST /webhooks/{webhook_id}/verify", webhookHandler.Verify)
	eventHandler := events.NewHandler(pool, limits.NewProjectRateLimiter(cfg.RateLimitPerSecond), cfg.MaxPendingDeliveries, metrics)
	eventTypeHandler := eventtypes.NewHandler(pool)
	mux.HandleFunc("POST /projects/{project_id}/events", eventHandler.Create)
	mux.HandleFunc("GET /projects/{project_id}/events", eventHandler.List)
	mux.HandleFunc("GET /events/{event_id}", eventHandler.Get)
	mux.HandleFunc("POST /projects/{project_id}/event-types", eventTypeHandler.Create)
	mux.HandleFunc("GET /projects/{project_id}/event-types", eventTypeHandler.List)
	mux.HandleFunc("PATCH /event-types/{event_type_id}", eventTypeHandler.Update)
	mux.HandleFunc("POST /events/{event_id}/replay", eventHandler.Replay)
	deliveryHandler := deliveries.NewHandler(pool)
	mux.HandleFunc("GET /events/{event_id}/deliveries", deliveryHandler.ByEvent)
	mux.HandleFunc("GET /webhooks/{webhook_id}/deliveries", deliveryHandler.ByWebhook)
	mux.HandleFunc("GET /deliveries/{delivery_id}", deliveryHandler.Get)
	mux.HandleFunc("GET /deliveries/{delivery_id}/attempts", deliveryHandler.Attempts)
	mux.HandleFunc("POST /deliveries/{delivery_id}/retry", deliveryHandler.Retry)
	mux.HandleFunc("POST /deliveries/retry-failed", deliveryHandler.BulkRetry)
	authHandler := auth.NewHandler(pool, cfg.BootstrapKey)
	mux.HandleFunc("POST /projects/{project_id}/api-keys", authHandler.Create)
	mux.HandleFunc("DELETE /api-keys/{api_key_id}", authHandler.Revoke)
	rootHandler := authHandler.Middleware(cfg.AuthRequired, loggingMiddleware(mux))

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           rootHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	appCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerDone := make(chan struct{})
	if pool != nil && cfg.WorkerEnabled {
		deliveryWorker := worker.NewWithParallelism(pool, log.Default(), limits.NewWebhookLimiter(cfg.MaxConcurrentDeliveries), metrics, cfg.AllowPrivateWebhookURLs, cfg.MaxConcurrentDeliveries)
		go func() {
			defer close(workerDone)
			deliveryWorker.Run(appCtx)
		}()
	} else {
		close(workerDone)
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	log.Printf("HookRelay listening on %s", cfg.Addr)
	select {
	case err := <-serverErrors:
		stop()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-appCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
		select {
		case <-workerDone:
		case <-shutdownCtx.Done():
			log.Printf("delivery worker did not stop before shutdown deadline")
		}
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
		writer := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(writer, r)
		entry, _ := json.Marshal(map[string]any{
			"method": r.Method, "path": r.URL.Path, "status": writer.status,
			"duration_ms": time.Since(started).Milliseconds(),
		})
		log.Print(string(entry))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}
