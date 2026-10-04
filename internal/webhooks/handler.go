package webhooks

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/pagination"
	"hookrelay/internal/security"
)

type Handler struct {
	db           *pgxpool.Pool
	allowPrivate bool
}

type webhook struct {
	ID                       string          `json:"id"`
	ProjectID                string          `json:"project_id"`
	EventType                string          `json:"event_type"`
	TargetURL                string          `json:"target_url"`
	Secret                   string          `json:"secret,omitempty"`
	Description              *string         `json:"description,omitempty"`
	Environment              string          `json:"environment"`
	CustomHeaders            json.RawMessage `json:"custom_headers"`
	Active                   bool            `json:"active"`
	LastDeliveryStatus       any             `json:"last_delivery_status"`
	LastSuccessfulDeliveryAt any             `json:"last_successful_delivery_at"`
	CreatedAt                string          `json:"created_at"`
	UpdatedAt                string          `json:"updated_at"`
}

type createRequest struct {
	EventType     string            `json:"event_type"`
	TargetURL     string            `json:"target_url"`
	Description   string            `json:"description"`
	Environment   string            `json:"environment"`
	CustomHeaders map[string]string `json:"custom_headers"`
}

type updateRequest struct {
	EventType     *string            `json:"event_type"`
	TargetURL     *string            `json:"target_url"`
	Description   *string            `json:"description"`
	Environment   *string            `json:"environment"`
	CustomHeaders *map[string]string `json:"custom_headers"`
	Active        *bool              `json:"active"`
}

type apiError struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(db *pgxpool.Pool) *Handler { return &Handler{db: db} }

func NewHandlerWithOptions(db *pgxpool.Pool, allowPrivate bool) *Handler {
	return &Handler{db: db, allowPrivate: allowPrivate}
}

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
	input.Environment = strings.TrimSpace(input.Environment)
	if input.Environment == "" {
		input.Environment = "production"
	}
	if err := validateTargetURL(input.TargetURL, h.allowPrivate); err != nil {
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
		INSERT INTO webhooks (project_id, event_type, target_url, secret, description, environment, custom_headers)
		SELECT $1::uuid, $2, $3, $4
		       , $5, $6, COALESCE($7::jsonb, '{}'::jsonb)
		WHERE EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)
		RETURNING id::text, project_id::text, event_type, target_url, secret, active,
		          description, environment, custom_headers, created_at::text, updated_at::text`,
		r.PathValue("project_id"), input.EventType, input.TargetURL, secret, nullableString(input.Description), input.Environment, input.CustomHeaders).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.TargetURL, &result.Secret, &result.Active,
		&result.Description, &result.Environment, &result.CustomHeaders, &result.CreatedAt, &result.UpdatedAt,
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

	page, err := pagination.Parse(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	query := `SELECT w.id::text, w.project_id::text, w.event_type, w.target_url, w.active, w.description, w.environment, w.custom_headers,
		(SELECT d.status FROM deliveries d WHERE d.webhook_id = w.id ORDER BY d.created_at DESC LIMIT 1),
		(SELECT MAX(d.delivered_at) FROM deliveries d WHERE d.webhook_id = w.id), w.created_at::text, w.updated_at::text FROM webhooks w WHERE w.project_id = $1::uuid`
	args := []any{r.PathValue("project_id")}
	if eventType := strings.TrimSpace(r.URL.Query().Get("event_type")); eventType != "" {
		query += ` AND event_type = $2`
		args = append(args, eventType)
	}
	if page.Cursor != nil {
		query += fmt.Sprintf(` AND (created_at, id) < ($%d::timestamptz, $%d::uuid)`, len(args)+1, len(args)+2)
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	query += ` ORDER BY created_at DESC`
	args = append(args, page.Limit+1)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := h.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list webhooks")
		return
	}
	defer rows.Close()

	result := make([]webhook, 0)
	for rows.Next() {
		var item webhook
		if err := rows.Scan(&item.ID, &item.ProjectID, &item.EventType, &item.TargetURL, &item.Active, &item.Description, &item.Environment, &item.CustomHeaders, &item.LastDeliveryStatus, &item.LastSuccessfulDeliveryAt, &item.CreatedAt, &item.UpdatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read webhooks")
			return
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read webhooks")
		return
	}
	next := ""
	if len(result) > page.Limit {
		last := result[page.Limit-1]
		next = pagination.Encode(last.CreatedAt, last.ID)
	}
	writeJSON(w, http.StatusOK, pagination.Build(result, page.Limit, next))
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
	if input.EventType == nil && input.TargetURL == nil && input.Description == nil && input.Environment == nil && input.CustomHeaders == nil && input.Active == nil {
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
		if err := validateTargetURL(value, h.allowPrivate); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.TargetURL = &value
	}
	if input.Environment != nil {
		value := strings.TrimSpace(*input.Environment)
		if value == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "environment cannot be empty")
			return
		}
		input.Environment = &value
	}

	var result webhook
	err := h.db.QueryRow(r.Context(), `
		UPDATE webhooks
		SET event_type = COALESCE($2, event_type), target_url = COALESCE($3, target_url),
		    description = COALESCE($4, description), environment = COALESCE($5, environment),
		    custom_headers = COALESCE($6::jsonb, custom_headers), active = COALESCE($7, active), updated_at = NOW()
		WHERE id = $1::uuid
		RETURNING id::text, project_id::text, event_type, target_url, secret, active, description, environment, custom_headers, created_at::text, updated_at::text`,
		r.PathValue("webhook_id"), input.EventType, input.TargetURL, input.Description, input.Environment, input.CustomHeaders, input.Active).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.TargetURL, &result.Secret, &result.Active, &result.Description, &result.Environment, &result.CustomHeaders, &result.CreatedAt, &result.UpdatedAt,
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

func (h *Handler) RotateSecret(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	secret, err := generateSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not generate webhook secret")
		return
	}
	var result webhook
	err = h.db.QueryRow(r.Context(), `UPDATE webhooks SET secret = $2, updated_at = NOW() WHERE id = $1::uuid RETURNING id::text, project_id::text, event_type, target_url, secret, active, description, environment, custom_headers, created_at::text, updated_at::text`, r.PathValue("webhook_id"), secret).Scan(
		&result.ID, &result.ProjectID, &result.EventType, &result.TargetURL, &result.Secret, &result.Active, &result.Description, &result.Environment, &result.CustomHeaders, &result.CreatedAt, &result.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "webhook not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not rotate webhook secret")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) Verify(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var targetURL string
	if err := h.db.QueryRow(r.Context(), `SELECT target_url FROM webhooks WHERE id = $1::uuid`, r.PathValue("webhook_id")).Scan(&targetURL); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "webhook not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not inspect webhook")
		return
	}
	client := security.NewClient(5*time.Second, h.allowPrivate)
	request, err := http.NewRequestWithContext(r.Context(), http.MethodHead, targetURL, nil)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"reachable": false, "status_code": nil, "error": err.Error()})
		return
	}
	response, err := client.Do(request)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"reachable": false, "status_code": nil, "error": err.Error()})
		return
	}
	defer response.Body.Close()
	writeJSON(w, http.StatusOK, map[string]any{"reachable": response.StatusCode < 500, "status_code": response.StatusCode})
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
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
	return validateTargetURL(value, true)
}

func validateTargetURL(value string, allowPrivate bool) error {
	return security.ValidateURL(value, allowPrivate)
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
