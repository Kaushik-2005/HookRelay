package worker

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/limits"
	"hookrelay/internal/observability"
	"hookrelay/internal/security"
)

const (
	pollInterval   = 5 * time.Second
	requestTimeout = 10 * time.Second
	maxAttempts    = 5
)

type Worker struct {
	db          *pgxpool.Pool
	client      *http.Client
	logger      *log.Logger
	concurrency *limits.WebhookLimiter
	metrics     *observability.Metrics
	slots       chan struct{}
}

type delivery struct {
	ID            string
	EventID       string
	WebhookID     string
	EventType     string
	Payload       json.RawMessage
	CreatedAt     string
	TargetURL     string
	Secret        string
	CustomHeaders json.RawMessage
	Attempt       int
	LeaseID       string
}

type webhookPayload struct {
	EventID   string          `json:"event_id"`
	EventType string          `json:"event_type"`
	CreatedAt string          `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

func New(db *pgxpool.Pool, logger *log.Logger, concurrency *limits.WebhookLimiter, metrics *observability.Metrics) *Worker {
	return NewWithOptions(db, logger, concurrency, metrics, false)
}

func NewWithOptions(db *pgxpool.Pool, logger *log.Logger, concurrency *limits.WebhookLimiter, metrics *observability.Metrics, allowPrivate bool) *Worker {
	return NewWithParallelism(db, logger, concurrency, metrics, allowPrivate, 1)
}

func NewWithParallelism(db *pgxpool.Pool, logger *log.Logger, concurrency *limits.WebhookLimiter, metrics *observability.Metrics, allowPrivate bool, parallelism int) *Worker {
	if logger == nil {
		logger = log.Default()
	}
	if parallelism < 1 {
		parallelism = 1
	}
	return &Worker{
		db:          db,
		client:      security.NewClient(requestTimeout, allowPrivate),
		logger:      logger,
		concurrency: concurrency,
		metrics:     metrics,
		slots:       make(chan struct{}, parallelism),
	}
}

func (w *Worker) Run(ctx context.Context) {
	if w.db == nil {
		return
	}
	w.logger.Printf("delivery worker started")
	w.processAvailable(ctx)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logger.Printf("delivery worker stopped")
			return
		case <-ticker.C:
			w.processAvailable(ctx)
		}
	}
}

func (w *Worker) processAvailable(ctx context.Context) {
	w.refreshQueueDepth(ctx)
	var active sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			active.Wait()
			return
		default:
		}
		item, claimed, err := w.claim(ctx)
		if err != nil {
			w.logger.Printf("claim delivery: %v", err)
			return
		}
		if !claimed {
			active.Wait()
			return
		}
		select {
		case w.slots <- struct{}{}:
		case <-ctx.Done():
			// The item is already claimed. Let it finish so shutdown does not
			// turn an in-flight request into an avoidable duplicate.
			active.Add(1)
			go func() {
				defer active.Done()
				deliveryCtx, cancel := w.deliveryContext(ctx)
				defer cancel()
				w.deliver(deliveryCtx, item)
			}()
			active.Wait()
			return
		}
		active.Add(1)
		go func(item delivery) {
			defer active.Done()
			defer func() { <-w.slots }()
			deliveryCtx, cancel := w.deliveryContext(ctx)
			defer cancel()
			w.deliver(deliveryCtx, item)
		}(item)
	}
}

func (w *Worker) deliveryContext(parent context.Context) (context.Context, context.CancelFunc) {
	// Claims stop immediately on shutdown, while an already-claimed HTTP
	// request receives a bounded independent context and can record its result.
	return context.WithTimeout(context.WithoutCancel(parent), requestTimeout+5*time.Second)
}

func (w *Worker) refreshQueueDepth(ctx context.Context) {
	if w.metrics == nil {
		return
	}
	var depth int64
	if err := w.db.QueryRow(ctx, `SELECT COUNT(*) FROM deliveries WHERE status = 'pending' OR (status = 'failed' AND next_retry_at <= NOW())`).Scan(&depth); err == nil {
		w.metrics.SetQueueDepth(depth)
	}
}

func (w *Worker) claim(ctx context.Context) (delivery, bool, error) {
	var item delivery
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return item, false, err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, `
		SELECT d.id::text, e.id::text, d.webhook_id::text, e.event_type, e.payload, e.created_at::text,
		       w.target_url, w.secret, w.custom_headers, d.attempt_count + 1
		FROM deliveries d
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE d.status = 'pending'
		   OR (d.status = 'failed' AND d.next_retry_at <= NOW())
		   OR (d.status = 'delivering' AND COALESCE(d.lease_expires_at, d.updated_at + INTERVAL '5 minutes') <= NOW())
		ORDER BY d.created_at
		FOR UPDATE OF d SKIP LOCKED
		LIMIT 1`).Scan(
		&item.ID, &item.EventID, &item.WebhookID, &item.EventType, &item.Payload, &item.CreatedAt,
		&item.TargetURL, &item.Secret, &item.CustomHeaders, &item.Attempt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return item, false, tx.Commit(ctx)
		}
		return item, false, err
	}

	if err = tx.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'delivering', attempt_count = $2, lease_id = gen_random_uuid(),
		    lease_expires_at = NOW() + INTERVAL '5 minutes', updated_at = NOW()
		WHERE id = $1::uuid
		RETURNING lease_id::text`, item.ID, item.Attempt).Scan(&item.LeaseID); err != nil {
		return item, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return item, false, err
	}
	return item, true, nil
}

func (w *Worker) deliver(ctx context.Context, item delivery) {
	started := time.Now()
	responseCode := 0
	success := false
	defer func() {
		if w.metrics != nil {
			w.metrics.ObserveDelivery(time.Since(started), success, retryable(responseCode) && item.Attempt < maxAttempts)
		}
	}()
	release := w.concurrency.Acquire(item.WebhookID)
	defer release()
	attemptID, err := w.startAttempt(ctx, item.ID, item.Attempt)
	if err != nil {
		w.logger.Printf("record delivery %s attempt: %v", item.ID, err)
	}
	finishAttempt := func(responseCode int, message string) {
		if attemptID != "" {
			w.finishAttempt(ctx, attemptID, responseCode, message, time.Since(started))
		}
	}
	body, err := json.Marshal(webhookPayload{
		EventID: item.EventID, EventType: item.EventType, CreatedAt: item.CreatedAt, Data: item.Payload,
	})
	if err != nil {
		finishAttempt(0, err.Error())
		w.markFailure(ctx, item.ID, item.LeaseID, item.Attempt, 0, err.Error())
		return
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	signature := sign(timestamp+"."+string(body), item.Secret)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, item.TargetURL, strings.NewReader(string(body)))
	if err != nil {
		finishAttempt(0, err.Error())
		w.markFailure(ctx, item.ID, item.LeaseID, item.Attempt, 0, err.Error())
		return
	}
	request.Header.Set("Content-Type", "application/json")
	var customHeaders map[string]string
	if len(item.CustomHeaders) > 0 && json.Unmarshal(item.CustomHeaders, &customHeaders) == nil {
		for name, value := range customHeaders {
			if isReservedHeader(name) {
				continue
			}
			request.Header.Set(name, value)
		}
	}
	request.Header.Set("X-HookRelay-Event-ID", item.EventID)
	request.Header.Set("X-HookRelay-Delivery-ID", item.ID)
	request.Header.Set("X-HookRelay-Timestamp", timestamp)
	request.Header.Set("X-HookRelay-Signature", signature)

	response, err := w.client.Do(request)
	if err != nil {
		finishAttempt(0, err.Error())
		w.markFailure(ctx, item.ID, item.LeaseID, item.Attempt, 0, err.Error())
		return
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	responseCode = response.StatusCode
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		success = true
		finishAttempt(response.StatusCode, "")
		w.markDelivered(ctx, item.ID, item.LeaseID, response.StatusCode)
		return
	}
	message := fmt.Sprintf("webhook returned HTTP %d", response.StatusCode)
	finishAttempt(response.StatusCode, message)
	w.markFailure(ctx, item.ID, item.LeaseID, item.Attempt, response.StatusCode, message)
}

func isReservedHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "content-length", "host", "connection", "transfer-encoding",
		"x-hookrelay-event-id", "x-hookrelay-delivery-id", "x-hookrelay-timestamp", "x-hookrelay-signature":
		return true
	default:
		return false
	}
}

func (w *Worker) startAttempt(ctx context.Context, deliveryID string, attemptNumber int) (string, error) {
	var id string
	err := w.db.QueryRow(ctx, `
		INSERT INTO delivery_attempts (delivery_id, attempt_number)
		VALUES ($1::uuid, $2)
		RETURNING id::text`, deliveryID, attemptNumber).Scan(&id)
	return id, err
}

func (w *Worker) finishAttempt(ctx context.Context, attemptID string, responseCode int, message string, duration time.Duration) {
	if len(message) > 1000 {
		message = message[:1000]
	}
	if _, err := w.db.Exec(ctx, `
		UPDATE delivery_attempts
		SET response_code = NULLIF($2, 0), error = NULLIF($3, ''), duration_ms = $4
		WHERE id = $1::uuid`, attemptID, responseCode, message, duration.Milliseconds()); err != nil {
		w.logger.Printf("finish delivery attempt %s: %v", attemptID, err)
	}
}

func (w *Worker) markDelivered(ctx context.Context, id, leaseID string, responseCode int) {
	result, err := w.db.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivered', response_code = $2, last_error = NULL,
		    next_retry_at = NULL, delivered_at = NOW(), lease_id = NULL,
		    lease_expires_at = NULL, updated_at = NOW()
		WHERE id = $1::uuid AND status = 'delivering' AND lease_id = $3::uuid`, id, responseCode, leaseID)
	if err != nil {
		w.logger.Printf("mark delivery %s delivered: %v", id, err)
	} else if result.RowsAffected() == 0 {
		w.logger.Printf("delivery %s lease expired before success was recorded", id)
	}
}

func (w *Worker) markFailure(ctx context.Context, id, leaseID string, attempt, responseCode int, message string) {
	if len(message) > 1000 {
		message = message[:1000]
	}
	var nextRetryAt *time.Time
	if retryable(responseCode) {
		if delay, ok := retryDelay(attempt); ok {
			next := time.Now().UTC().Add(delay)
			nextRetryAt = &next
		}
	}
	result, err := w.db.Exec(ctx, `
		UPDATE deliveries
		SET status = 'failed', response_code = NULLIF($2, 0), last_error = $3,
		    next_retry_at = $4, lease_id = NULL, lease_expires_at = NULL, updated_at = NOW()
		WHERE id = $1::uuid AND status = 'delivering' AND lease_id = $5::uuid`, id, responseCode, message, nextRetryAt, leaseID)
	if err != nil {
		w.logger.Printf("mark delivery %s failed: %v", id, err)
	} else if result.RowsAffected() == 0 {
		w.logger.Printf("delivery %s lease expired before failure was recorded", id)
	}
}

func retryable(responseCode int) bool {
	return responseCode == 0 || responseCode == http.StatusRequestTimeout ||
		responseCode == http.StatusConflict || responseCode == http.StatusTooManyRequests ||
		responseCode >= 500
}

func retryDelay(attempt int) (time.Duration, bool) {
	if attempt >= maxAttempts {
		return 0, false
	}
	switch attempt {
	case 1:
		return time.Minute, true
	case 2:
		return 5 * time.Minute, true
	default:
		return 15 * time.Minute, true
	}
}

func sign(input, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(input))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
