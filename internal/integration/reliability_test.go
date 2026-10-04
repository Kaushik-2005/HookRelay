package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/db"
	"hookrelay/internal/events"
	"hookrelay/internal/limits"
	"hookrelay/internal/observability"
	"hookrelay/internal/worker"
)

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	poolConfig.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(context.Background(), pool, "../../migrations"); err != nil {
		t.Fatalf("migrate database: %v", err)
	}
	return pool
}

func createProject(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(), `INSERT INTO projects (name) VALUES ($1) RETURNING id::text`, "integration-"+strconv.FormatInt(time.Now().UnixNano(), 10)).Scan(&id); err != nil {
		t.Fatalf("create project: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM delivery_attempts WHERE delivery_id IN (SELECT d.id FROM deliveries d JOIN events e ON e.id = d.event_id WHERE e.project_id = $1::uuid)`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM deliveries WHERE event_id IN (SELECT id FROM events WHERE project_id = $1::uuid)`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM events WHERE project_id = $1::uuid`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM webhooks WHERE project_id = $1::uuid`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM projects WHERE id = $1::uuid`, id)
	})
	return id
}

func createWebhook(t *testing.T, pool *pgxpool.Pool, projectID, targetURL string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO webhooks (project_id, event_type, target_url, secret)
		VALUES ($1::uuid, 'payment.succeeded', $2, 'integration-secret')
		RETURNING id::text`, projectID, targetURL).Scan(&id); err != nil {
		t.Fatalf("create webhook: %v", err)
	}
	return id
}

func TestConcurrentIdempotencyKeys(t *testing.T) {
	pool := integrationPool(t)
	projectID := createProject(t, pool)
	handler := events.NewHandler(pool, limits.NewProjectRateLimiter(0), 0, nil)

	const callers = 20
	responses := make(chan struct {
		status int
		id     string
	}, callers)
	var group sync.WaitGroup
	for i := 0; i < callers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			req := httptest.NewRequest(http.MethodPost, "/projects/"+projectID+"/events", bytes.NewBufferString(`{"event_type":"payment.succeeded","payload":{"payment_id":"same"}}`))
			req.SetPathValue("project_id", projectID)
			req.Header.Set("Idempotency-Key", "integration-key")
			recorder := httptest.NewRecorder()
			handler.Create(recorder, req)
			var result struct {
				ID string `json:"id"`
			}
			_ = json.NewDecoder(recorder.Body).Decode(&result)
			responses <- struct {
				status int
				id     string
			}{recorder.Code, result.ID}
		}()
	}
	group.Wait()
	close(responses)

	var eventID string
	for response := range responses {
		if response.status != http.StatusCreated && response.status != http.StatusOK {
			t.Fatalf("concurrent idempotent publish returned HTTP %d", response.status)
		}
		if response.id == "" {
			t.Fatal("concurrent idempotent publish returned no event ID")
		}
		if eventID == "" {
			eventID = response.id
		} else if eventID != response.id {
			t.Fatalf("idempotency produced multiple event IDs: %s and %s", eventID, response.id)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM events WHERE project_id = $1::uuid`, projectID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one event, got %d", count)
	}
}

func TestWorkersClaimAndRecoverLeases(t *testing.T) {
	pool := integrationPool(t)
	projectID := createProject(t, pool)
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	webhookID := createWebhook(t, pool, projectID, receiver.URL)

	var eventID, deliveryID string
	err := pool.QueryRow(context.Background(), `INSERT INTO events (project_id, event_type, payload) VALUES ($1::uuid, 'payment.succeeded', '{"source":"integration"}') RETURNING id::text`, projectID).Scan(&eventID)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `INSERT INTO deliveries (event_id, webhook_id) VALUES ($1::uuid, $2::uuid) RETURNING id::text`, eventID, webhookID).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := worker.NewWithParallelism(pool, nil, limits.NewWebhookLimiter(2), observability.NewMetrics(), true, 2)
	second := worker.NewWithParallelism(pool, nil, limits.NewWebhookLimiter(2), observability.NewMetrics(), true, 2)
	var group sync.WaitGroup
	group.Add(2)
	go func() { defer group.Done(); first.Run(ctx) }()
	go func() { defer group.Done(); second.Run(ctx) }()
	waitForStatus(t, pool, deliveryID, "delivered")
	cancel()
	group.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one outbound delivery despite two workers, got %d", got)
	}

	// A stale delivering row represents a worker that crashed after claiming.
	var recoveredDelivery string
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO events (project_id, event_type, payload) VALUES ($1::uuid, 'payment.succeeded', '{"source":"crash"}') RETURNING id::text`, projectID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `INSERT INTO deliveries (event_id, webhook_id, status, lease_id, lease_expires_at) VALUES ($1::uuid, $2::uuid, 'delivering', gen_random_uuid(), NOW() - INTERVAL '1 minute') RETURNING id::text`, eventID, webhookID).Scan(&recoveredDelivery); err != nil {
		t.Fatal(err)
	}
	recoveryCtx, stopRecovery := context.WithCancel(context.Background())
	go worker.NewWithParallelism(pool, nil, limits.NewWebhookLimiter(1), observability.NewMetrics(), true, 1).Run(recoveryCtx)
	waitForStatus(t, pool, recoveredDelivery, "delivered")
	stopRecovery()
}

func TestFailureScenarioMeasurements(t *testing.T) {
	pool := integrationPool(t)
	type scenario struct {
		name       string
		statusCode int
		target     string
		close      func()
	}
	var scenarios []scenario
	for _, code := range []int{http.StatusTooManyRequests, http.StatusInternalServerError} {
		code := code
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		scenarios = append(scenarios, scenario{name: fmt.Sprintf("http_%d", code), statusCode: code, target: server.URL, close: server.Close})
	}
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	scenarios = append(scenarios, scenario{name: "unavailable", target: closedURL, close: func() {}})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(11 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	scenarios = append(scenarios, scenario{name: "slow_timeout", target: slow.URL, close: slow.Close})
	defer func() {
		for _, item := range scenarios {
			item.close()
		}
	}()

	for _, item := range scenarios {
		item := item
		t.Run(item.name, func(t *testing.T) {
			projectID := createProject(t, pool)
			webhookID := createWebhook(t, pool, projectID, item.target)
			var eventID, deliveryID string
			if err := pool.QueryRow(context.Background(), `INSERT INTO events (project_id, event_type, payload) VALUES ($1::uuid, 'payment.succeeded', '{"scenario":true}') RETURNING id::text`, projectID).Scan(&eventID); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(context.Background(), `INSERT INTO deliveries (event_id, webhook_id) VALUES ($1::uuid, $2::uuid) RETURNING id::text`, eventID, webhookID).Scan(&deliveryID); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			ctx, cancel := context.WithCancel(context.Background())
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				worker.NewWithParallelism(pool, nil, limits.NewWebhookLimiter(1), observability.NewMetrics(), true, 1).Run(ctx)
			}()
			waitForStatus(t, pool, deliveryID, "failed")
			elapsed := time.Since(started)
			cancel()
			<-workerDone

			var responseCode *int
			var attempts int
			var nextRetryAt any
			if err := pool.QueryRow(context.Background(), `SELECT response_code, attempt_count, next_retry_at FROM deliveries WHERE id = $1::uuid`, deliveryID).Scan(&responseCode, &attempts, &nextRetryAt); err != nil {
				t.Fatal(err)
			}
			if attempts != 1 {
				t.Fatalf("expected one initial attempt, got %d", attempts)
			}
			if nextRetryAt == nil {
				t.Fatalf("expected retry to be scheduled")
			}
			if item.statusCode != 0 && (responseCode == nil || *responseCode != item.statusCode) {
				t.Fatalf("expected response code %d, got %v", item.statusCode, responseCode)
			}
			t.Logf("elapsed=%s response_code=%v attempts=%d retry_scheduled=true", elapsed.Round(time.Millisecond), responseCode, attempts)
		})
	}
}

func waitForStatus(t *testing.T, pool *pgxpool.Pool, deliveryID, expected string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		if err := pool.QueryRow(context.Background(), `SELECT status FROM deliveries WHERE id = $1::uuid`, deliveryID).Scan(&status); err == nil && status == expected {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	var status string
	_ = pool.QueryRow(context.Background(), `SELECT status FROM deliveries WHERE id = $1::uuid`, deliveryID).Scan(&status)
	t.Fatalf("delivery %s did not reach %q; status=%q", deliveryID, expected, status)
}

func BenchmarkEventPublishAndDelivery(b *testing.B) {
	if os.Getenv("DATABASE_URL") == "" {
		b.Skip("DATABASE_URL is not set; skipping PostgreSQL delivery benchmark")
	}
	poolConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		b.Fatal(err)
	}
	poolConfig.MaxConns = 50
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(context.Background(), pool, "../../migrations"); err != nil {
		b.Fatal(err)
	}
	projectID := createProjectBenchmark(b, pool)
	var delivered atomic.Int64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)
	publishedAt := make(map[int64]time.Time, b.N)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var envelope struct {
			Data struct {
				Sequence int64 `json:"sequence"`
			} `json:"data"`
		}
		_ = json.NewDecoder(r.Body).Decode(&envelope)
		now := time.Now()
		latencyMu.Lock()
		if started, ok := publishedAt[envelope.Data.Sequence]; ok {
			latencies = append(latencies, now.Sub(started))
		}
		latencyMu.Unlock()
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	createWebhookBenchmark(b, pool, projectID, receiver.URL)
	parallelism := 1
	if value, err := strconv.Atoi(os.Getenv("HOOKRELAY_BENCH_WORKER_CONCURRENCY")); err == nil && value > 0 {
		parallelism = value
	}
	handler := events.NewHandler(pool, limits.NewProjectRateLimiter(0), 0, nil)
	b.ResetTimer()
	started := time.Now()
	for i := 0; i < b.N; i++ {
		latencyMu.Lock()
		publishedAt[int64(i)] = time.Now()
		latencyMu.Unlock()
		req := httptest.NewRequest(http.MethodPost, "/projects/"+projectID+"/events", bytes.NewBufferString(fmt.Sprintf(`{"event_type":"payment.succeeded","payload":{"sequence":%d}}`, i)))
		req.SetPathValue("project_id", projectID)
		recorder := httptest.NewRecorder()
		handler.Create(recorder, req)
		if recorder.Code != http.StatusCreated {
			b.Fatalf("publish returned HTTP %d", recorder.Code)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.NewWithParallelism(pool, nil, limits.NewWebhookLimiter(parallelism), observability.NewMetrics(), true, parallelism).Run(ctx)
	}()
	defer func() {
		cancel()
		<-workerDone
		pool.Close()
	}()
	b.StopTimer()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && delivered.Load() < int64(b.N) {
		time.Sleep(5 * time.Millisecond)
	}
	if delivered.Load() < int64(b.N) {
		b.Fatalf("only %d/%d deliveries completed", delivered.Load(), b.N)
	}
	endToEndDuration := time.Since(started)
	latencyMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	measured := append([]time.Duration(nil), latencies...)
	latencyMu.Unlock()
	if len(measured) > 0 {
		b.ReportMetric(float64(percentileDuration(measured, .50).Microseconds()), "delivery-p50-us")
		b.ReportMetric(float64(percentileDuration(measured, .95).Microseconds()), "delivery-p95-us")
		b.ReportMetric(float64(percentileDuration(measured, .99).Microseconds()), "delivery-p99-us")
	}
	var queueP50, queueP95, queueP99, httpP50, httpP95, httpP99 float64
	if err := pool.QueryRow(context.Background(), `
		SELECT
			percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.created_at - e.created_at)) * 1000),
			percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.created_at - e.created_at)) * 1000),
			percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.created_at - e.created_at)) * 1000),
			percentile_cont(0.50) WITHIN GROUP (ORDER BY a.duration_ms),
			percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms),
			percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms)
		FROM delivery_attempts a
		JOIN deliveries d ON d.id = a.delivery_id
		JOIN events e ON e.id = d.event_id
		WHERE e.project_id = $1::uuid AND a.attempt_number = 1`, projectID).Scan(
		&queueP50, &queueP95, &queueP99, &httpP50, &httpP95, &httpP99); err != nil {
		b.Fatalf("read delivery timing percentiles: %v", err)
	}
	b.ReportMetric(queueP50, "queue-p50-ms")
	b.ReportMetric(queueP95, "queue-p95-ms")
	b.ReportMetric(queueP99, "queue-p99-ms")
	b.ReportMetric(httpP50, "http-p50-ms")
	b.ReportMetric(httpP95, "http-p95-ms")
	b.ReportMetric(httpP99, "http-p99-ms")
	b.ReportMetric(float64(b.N)/endToEndDuration.Seconds(), "deliveries/sec")
	b.ReportMetric(float64(delivered.Load()), "deliveries")
}

func percentileDuration(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	return values[int(float64(len(values)-1)*percentile)]
}

func createProjectBenchmark(b *testing.B, pool *pgxpool.Pool) string {
	var id string
	if err := pool.QueryRow(context.Background(), `INSERT INTO projects (name) VALUES ($1) RETURNING id::text`, "benchmark").Scan(&id); err != nil {
		b.Fatal(err)
	}
	return id
}

func createWebhookBenchmark(b *testing.B, pool *pgxpool.Pool, projectID, targetURL string) string {
	var id string
	if err := pool.QueryRow(context.Background(), `INSERT INTO webhooks (project_id, event_type, target_url, secret) VALUES ($1::uuid, 'payment.succeeded', $2, 'benchmark-secret') RETURNING id::text`, projectID, targetURL).Scan(&id); err != nil {
		b.Fatal(err)
	}
	return id
}
