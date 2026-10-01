package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct{ db *pgxpool.Pool }

type event struct {
	ID        string          `json:"id"`
	ProjectID string          `json:"project_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt string          `json:"created_at"`
}

type createRequest struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type apiError struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(db *pgxpool.Pool) *Handler { return &Handler{db: db} }

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}

	var input createRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 5<<20)).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	input.EventType = strings.TrimSpace(input.EventType)
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 255 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Idempotency-Key must be 255 characters or fewer")
		return
	}
	if input.EventType == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "event_type is required")
		return
	}
	if len(input.Payload) == 0 || bytes.Equal(bytes.TrimSpace(input.Payload), []byte("null")) {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload is required")
		return
	}
	if !json.Valid(input.Payload) {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload must be valid JSON")
		return
	}
	fingerprint := requestFingerprint(input.EventType, input.Payload)

	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not start event transaction")
		return
	}
	defer tx.Rollback(r.Context())

	var projectExists bool
	if err := tx.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)`, r.PathValue("project_id")).Scan(&projectExists); err != nil || !projectExists {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	if idempotencyKey != "" {
		var existing event
		var existingFingerprint string
		err = tx.QueryRow(r.Context(), `
			SELECT id::text, project_id::text, event_type, payload, created_at::text, request_fingerprint
			FROM events
			WHERE project_id = $1::uuid AND idempotency_key = $2
			FOR UPDATE`, r.PathValue("project_id"), idempotencyKey).Scan(
			&existing.ID, &existing.ProjectID, &existing.EventType, &existing.Payload, &existing.CreatedAt, &existingFingerprint,
		)
		if err == nil {
			if existingFingerprint != fingerprint {
				writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used with a different request")
				return
			}
			if err = tx.Commit(r.Context()); err != nil {
				writeError(w, http.StatusInternalServerError, "internal_error", "could not commit idempotent request")
				return
			}
			writeJSON(w, http.StatusOK, existing)
			return
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not check idempotency key")
			return
		}
	}

	var result event
	err = tx.QueryRow(r.Context(), `
		INSERT INTO events (project_id, event_type, payload, idempotency_key, request_fingerprint)
		VALUES ($1::uuid, $2, $3::jsonb, NULLIF($4, ''), NULLIF($5, ''))
		RETURNING id::text, project_id::text, event_type, payload, created_at::text`,
		r.PathValue("project_id"), input.EventType, input.Payload, idempotencyKey, fingerprint).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.Payload, &result.CreatedAt,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create event")
		return
	}

	if _, err = tx.Exec(r.Context(), `
		INSERT INTO deliveries (event_id, webhook_id)
		SELECT $1::uuid, id FROM webhooks
		WHERE project_id = $2::uuid AND event_type = $3 AND active = TRUE`,
		result.ID, result.ProjectID, result.EventType); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create deliveries")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not commit event")
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func requestFingerprint(eventType string, payload json.RawMessage) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s", eventType, payload)))
	return fmt.Sprintf("%x", hash[:])
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var exists bool
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)`, r.PathValue("project_id")).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	rows, err := h.db.Query(r.Context(), `
		SELECT id::text, project_id::text, event_type, payload, created_at::text
		FROM events WHERE project_id = $1::uuid ORDER BY created_at DESC`, r.PathValue("project_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list events")
		return
	}
	defer rows.Close()
	result := make([]event, 0)
	for rows.Next() {
		var item event
		if err := rows.Scan(&item.ID, &item.ProjectID, &item.EventType, &item.Payload, &item.CreatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read events")
			return
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read events")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var result event
	err := h.db.QueryRow(r.Context(), `
		SELECT id::text, project_id::text, event_type, payload, created_at::text
		FROM events WHERE id = $1::uuid`, r.PathValue("event_id")).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.Payload, &result.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "event not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not get event")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Error: errorBody{Code: code, Message: message}})
}
