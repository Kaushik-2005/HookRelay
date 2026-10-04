package eventtypes

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/pagination"
)

type Handler struct{ db *pgxpool.Pool }

type eventType struct {
	ID          string          `json:"id"`
	ProjectID   string          `json:"project_id"`
	Name        string          `json:"name"`
	Description *string         `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type createRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
}

type updateRequest struct {
	Description *string          `json:"description"`
	Schema      *json.RawMessage `json:"schema"`
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
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	if len(input.Schema) > 0 && !json.Valid(input.Schema) {
		writeError(w, http.StatusBadRequest, "invalid_request", "schema must be valid JSON")
		return
	}
	var result eventType
	err := h.db.QueryRow(r.Context(), `
		INSERT INTO event_types (project_id, name, description, schema)
		SELECT $1::uuid, $2, NULLIF($3, ''), NULLIF($4::jsonb, 'null'::jsonb)
		WHERE EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)
		RETURNING id::text, project_id::text, name, description, COALESCE(schema, 'null'::jsonb), created_at::text, updated_at::text`,
		r.PathValue("project_id"), input.Name, strings.TrimSpace(input.Description), nullableJSON(input.Schema)).Scan(
		&result.ID, &result.ProjectID, &result.Name, &result.Description, &result.Schema, &result.CreatedAt, &result.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, "conflict", "event type already exists or could not be created")
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
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)`, r.PathValue("project_id")).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	page, err := pagination.Parse(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	rows, err := h.db.Query(r.Context(), `
		SELECT id::text, project_id::text, name, description, COALESCE(schema, 'null'::jsonb), created_at::text, updated_at::text
		FROM event_types WHERE project_id = $1::uuid ORDER BY created_at DESC LIMIT $2`, r.PathValue("project_id"), page.Limit+1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list event types")
		return
	}
	defer rows.Close()
	result := make([]eventType, 0)
	for rows.Next() {
		var item eventType
		if err := rows.Scan(&item.ID, &item.ProjectID, &item.Name, &item.Description, &item.Schema, &item.CreatedAt, &item.UpdatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read event types")
			return
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read event types")
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil || (input.Description == nil && input.Schema == nil) {
		writeError(w, http.StatusBadRequest, "invalid_request", "description or schema is required")
		return
	}
	if input.Schema != nil && !json.Valid(*input.Schema) {
		writeError(w, http.StatusBadRequest, "invalid_request", "schema must be valid JSON")
		return
	}
	var result eventType
	err := h.db.QueryRow(r.Context(), `
		UPDATE event_types SET description = COALESCE($2, description), schema = COALESCE($3::jsonb, schema), updated_at = NOW()
		WHERE id = $1::uuid
		RETURNING id::text, project_id::text, name, description, COALESCE(schema, 'null'::jsonb), created_at::text, updated_at::text`,
		r.PathValue("event_type_id"), input.Description, nullableJSONPointer(input.Schema)).Scan(
		&result.ID, &result.ProjectID, &result.Name, &result.Description, &result.Schema, &result.CreatedAt, &result.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "event type not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not update event type")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nullableJSONPointer(value *json.RawMessage) any {
	if value == nil {
		return nil
	}
	return *value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Error: errorBody{Code: code, Message: message}})
}
