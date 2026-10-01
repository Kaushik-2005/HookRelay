package webhooks

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct{ db *pgxpool.Pool }

type webhook struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	EventType string `json:"event_type"`
	TargetURL string `json:"target_url"`
	Secret    string `json:"secret,omitempty"`
	Active    bool   `json:"active"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type createRequest struct {
	EventType string `json:"event_type"`
	TargetURL string `json:"target_url"`
}

type updateRequest struct {
	EventType *string `json:"event_type"`
	TargetURL *string `json:"target_url"`
	Active    *bool   `json:"active"`
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	input.EventType = strings.TrimSpace(input.EventType)
	input.TargetURL = strings.TrimSpace(input.TargetURL)
	if input.EventType == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "event_type is required")
		return
	}
	if err := validateURL(input.TargetURL); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var projectExists bool
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)`, r.PathValue("project_id")).Scan(&projectExists); err != nil || !projectExists {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}

	secret, err := generateSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not generate webhook secret")
		return
	}

	var result webhook
	err = h.db.QueryRow(r.Context(), `
		INSERT INTO webhooks (project_id, event_type, target_url, secret)
		SELECT $1::uuid, $2, $3, $4
		WHERE EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)
		RETURNING id::text, project_id::text, event_type, target_url, secret, active,
		          created_at::text, updated_at::text`,
		r.PathValue("project_id"), input.EventType, input.TargetURL, secret).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.TargetURL, &result.Secret,
		&result.Active, &result.CreatedAt, &result.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create webhook")
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}

	var exists bool
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)`, r.PathValue("project_id")).Scan(&exists); err != nil {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}

	rows, err := h.db.Query(r.Context(), `
		SELECT id::text, project_id::text, event_type, target_url, active,
		       created_at::text, updated_at::text
		FROM webhooks WHERE project_id = $1::uuid ORDER BY created_at DESC`, r.PathValue("project_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list webhooks")
		return
	}
	defer rows.Close()

	result := make([]webhook, 0)
	for rows.Next() {
		var item webhook
		if err := rows.Scan(&item.ID, &item.ProjectID, &item.EventType, &item.TargetURL, &item.Active, &item.CreatedAt, &item.UpdatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read webhooks")
			return
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read webhooks")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var input updateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if input.EventType == nil && input.TargetURL == nil && input.Active == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "at least one field is required")
		return
	}
	if input.EventType != nil {
		value := strings.TrimSpace(*input.EventType)
		if value == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "event_type cannot be empty")
			return
		}
		input.EventType = &value
	}
	if input.TargetURL != nil {
		value := strings.TrimSpace(*input.TargetURL)
		if err := validateURL(value); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.TargetURL = &value
	}

	var result webhook
	err := h.db.QueryRow(r.Context(), `
		UPDATE webhooks
		SET event_type = COALESCE($2, event_type),
		    target_url = COALESCE($3, target_url),
		    active = COALESCE($4, active), updated_at = NOW()
		WHERE id = $1::uuid
		RETURNING id::text, project_id::text, event_type, target_url, secret, active,
		          created_at::text, updated_at::text`,
		r.PathValue("webhook_id"), input.EventType, input.TargetURL, input.Active).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.TargetURL, &result.Secret,
		&result.Active, &result.CreatedAt, &result.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "webhook not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not update webhook")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	command, err := h.db.Exec(r.Context(), `UPDATE webhooks SET active = FALSE, updated_at = NOW() WHERE id = $1::uuid`, r.PathValue("webhook_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not disable webhook")
		return
	}
	if command.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "not_found", "webhook not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validateURL(value string) error {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("target_url must be a valid http or https URL")
	}
	return nil
}

func generateSecret() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Error: errorBody{Code: code, Message: message}})
}
