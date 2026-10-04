package deliveries

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/pagination"
)

type Handler struct{ db *pgxpool.Pool }

type delivery struct {
	ID           string `json:"id"`
	EventID      string `json:"event_id"`
	WebhookID    string `json:"webhook_id"`
	Status       string `json:"status"`
	AttemptCount int    `json:"attempt_count"`
	NextRetryAt  any    `json:"next_retry_at"`
	ResponseCode any    `json:"response_code"`
	LastError    any    `json:"last_error"`
	DeliveredAt  any    `json:"delivered_at"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

type apiError struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(db *pgxpool.Pool) *Handler { return &Handler{db: db} }

func (h *Handler) ByEvent(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var exists bool
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1::uuid)`, r.PathValue("event_id")).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "not_found", "event not found")
		return
	}
	page, err := pagination.Parse(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	query := deliveryQuery + ` WHERE d.event_id = $1::uuid`
	args := []any{r.PathValue("event_id")}
	if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
		query += ` AND d.status = $2`
		args = append(args, status)
	}
	if page.Cursor != nil {
		query += fmt.Sprintf(` AND (d.created_at, d.id) < ($%d::timestamptz, $%d::uuid)`, len(args)+1, len(args)+2)
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	query += ` ORDER BY d.created_at DESC`
	args = append(args, page.Limit+1)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := h.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list deliveries")
		return
	}
	writeRows(w, rows, page)
}

func (h *Handler) ByWebhook(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var exists bool
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM webhooks WHERE id = $1::uuid)`, r.PathValue("webhook_id")).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "not_found", "webhook not found")
		return
	}
	page, err := pagination.Parse(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	query := deliveryQuery + ` WHERE d.webhook_id = $1::uuid`
	args := []any{r.PathValue("webhook_id")}
	if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
		query += ` AND d.status = $2`
		args = append(args, status)
	}
	if page.Cursor != nil {
		query += fmt.Sprintf(` AND (d.created_at, d.id) < ($%d::timestamptz, $%d::uuid)`, len(args)+1, len(args)+2)
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	query += ` ORDER BY d.created_at DESC`
	args = append(args, page.Limit+1)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := h.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list deliveries")
		return
	}
	writeRows(w, rows, page)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var item delivery
	err := h.db.QueryRow(r.Context(), deliveryQuery+` WHERE d.id = $1::uuid`, r.PathValue("delivery_id")).Scan(deliveryArgs(&item)...)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not get delivery")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) Retry(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not start retry")
		return
	}
	defer tx.Rollback(r.Context())

	var currentStatus string
	err = tx.QueryRow(r.Context(), `SELECT status FROM deliveries WHERE id = $1::uuid FOR UPDATE`, r.PathValue("delivery_id")).Scan(&currentStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not inspect delivery")
		return
	}
	if currentStatus != "failed" {
		writeError(w, http.StatusConflict, "invalid_state", "only failed deliveries can be retried")
		return
	}

	if _, err = tx.Exec(r.Context(), `
		UPDATE deliveries
		SET status = 'pending', next_retry_at = NOW(), lease_id = NULL,
		    lease_expires_at = NULL, updated_at = NOW()
		WHERE id = $1::uuid`, r.PathValue("delivery_id")); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not retry delivery")
		return
	}
	var item delivery
	if err = tx.QueryRow(r.Context(), deliveryQuery+` WHERE d.id = $1::uuid`, r.PathValue("delivery_id")).Scan(deliveryArgs(&item)...); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read retried delivery")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not commit retry")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

const deliveryQuery = `
	SELECT d.id::text, d.event_id::text, d.webhook_id::text, d.status,
	       d.attempt_count, d.next_retry_at, d.response_code, d.last_error,
	       d.delivered_at, d.created_at::text, d.updated_at::text
	FROM deliveries d`

func writeRows(w http.ResponseWriter, rows pgx.Rows, page pagination.Params) {
	defer rows.Close()
	result := make([]delivery, 0)
	for rows.Next() {
		var item delivery
		if err := rows.Scan(deliveryArgs(&item)...); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read deliveries")
			return
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read deliveries")
		return
	}
	next := ""
	if len(result) > page.Limit {
		last := result[page.Limit-1]
		next = pagination.Encode(last.CreatedAt, last.ID)
	}
	writeJSON(w, http.StatusOK, pagination.Build(result, page.Limit, next))
}

func deliveryArgs(item *delivery) []any {
	return []any{&item.ID, &item.EventID, &item.WebhookID, &item.Status, &item.AttemptCount,
		&item.NextRetryAt, &item.ResponseCode, &item.LastError, &item.DeliveredAt, &item.CreatedAt, &item.UpdatedAt}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Error: errorBody{Code: code, Message: message}})
}
