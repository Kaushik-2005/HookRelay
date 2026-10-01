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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pollInterval   = 5 * time.Second
	requestTimeout = 10 * time.Second
	maxAttempts    = 5
)

type Worker struct {
	db     *pgxpool.Pool
	client *http.Client
	logger *log.Logger
}

type delivery struct {
	ID        string
	EventID   string
	EventType string
	Payload   json.RawMessage
	CreatedAt string
	TargetURL string
	Secret    string
	Attempt   int
}

type webhookPayload struct {
	EventID   string          `json:"event_id"`
	EventType string          `json:"event_type"`
	CreatedAt string          `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

func New(db *pgxpool.Pool, logger *log.Logger) *Worker {
	if logger == nil {
		logger = log.Default()
	}
	return &Worker{
		db:     db,
		client: &http.Client{Timeout: requestTimeout},
		logger: logger,
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
	for {
		item, claimed, err := w.claim(ctx)
		if err != nil {
			w.logger.Printf("claim delivery: %v", err)
			return
		}
		if !claimed {
			return
		}
		w.deliver(ctx, item)
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
		SELECT d.id::text, e.id::text, e.event_type, e.payload, e.created_at::text,
		       w.target_url, w.secret, d.attempt_count + 1
		FROM deliveries d
		JOIN events e ON e.id = d.event_id
		JOIN webhooks w ON w.id = d.webhook_id
		WHERE d.status = 'pending'
		   OR (d.status = 'failed' AND d.next_retry_at <= NOW())
		   OR (d.status = 'delivering' AND d.updated_at < NOW() - INTERVAL '5 minutes')
		ORDER BY d.created_at
		FOR UPDATE OF d SKIP LOCKED
		LIMIT 1`).Scan(
		&item.ID, &item.EventID, &item.EventType, &item.Payload, &item.CreatedAt,
		&item.TargetURL, &item.Secret, &item.Attempt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return item, false, tx.Commit(ctx)
		}
		return item, false, err
	}

	if _, err = tx.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivering', attempt_count = $2, updated_at = NOW()
		WHERE id = $1::uuid`, item.ID, item.Attempt); err != nil {
		return item, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return item, false, err
	}
	return item, true, nil
}

func (w *Worker) deliver(ctx context.Context, item delivery) {
	body, err := json.Marshal(webhookPayload{
		EventID: item.EventID, EventType: item.EventType, CreatedAt: item.CreatedAt, Data: item.Payload,
	})
	if err != nil {
		w.markFailure(ctx, item.ID, item.Attempt, 0, err.Error())
		return
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	signature := sign(timestamp+"."+string(body), item.Secret)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, item.TargetURL, strings.NewReader(string(body)))
	if err != nil {
		w.markFailure(ctx, item.ID, item.Attempt, 0, err.Error())
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-HookRelay-Event-ID", item.EventID)
	request.Header.Set("X-HookRelay-Timestamp", timestamp)
	request.Header.Set("X-HookRelay-Signature", signature)

	response, err := w.client.Do(request)
	if err != nil {
		w.markFailure(ctx, item.ID, item.Attempt, 0, err.Error())
		return
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		w.markDelivered(ctx, item.ID, response.StatusCode)
		return
	}
	w.markFailure(ctx, item.ID, item.Attempt, response.StatusCode, fmt.Sprintf("webhook returned HTTP %d", response.StatusCode))
}

func (w *Worker) markDelivered(ctx context.Context, id string, responseCode int) {
	if _, err := w.db.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivered', response_code = $2, last_error = NULL,
		    next_retry_at = NULL, delivered_at = NOW(), updated_at = NOW()
		WHERE id = $1::uuid`, id, responseCode); err != nil {
		w.logger.Printf("mark delivery %s delivered: %v", id, err)
	}
}

func (w *Worker) markFailure(ctx context.Context, id string, attempt, responseCode int, message string) {
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
	if _, err := w.db.Exec(ctx, `
		UPDATE deliveries
		SET status = 'failed', response_code = NULLIF($2, 0), last_error = $3,
		    next_retry_at = $4, updated_at = NOW()
		WHERE id = $1::uuid`, id, responseCode, message, nextRetryAt); err != nil {
		w.logger.Printf("mark delivery %s failed: %v", id, err)
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
