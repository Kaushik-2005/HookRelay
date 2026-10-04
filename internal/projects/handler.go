package projects

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"hookrelay/internal/auth"
	"hookrelay/internal/pagination"
)

type Handler struct{ db *pgxpool.Pool }

type project struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type createRequest struct {
	Name string `json:"name"`
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
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if strings.TrimSpace(input.Name) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}

	var p project
	err := h.db.QueryRow(r.Context(), `
		INSERT INTO projects (name) VALUES ($1)
		RETURNING id::text, name, created_at::text, updated_at::text`, strings.TrimSpace(input.Name)).Scan(
		&p.ID, &p.Name, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create project")
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}

	page, err := pagination.Parse(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	query := `SELECT id::text, name, created_at::text, updated_at::text FROM projects`
	args := []any{}
	conditions := []string{}
	if projectID, ok := auth.ProjectID(r.Context()); ok {
		conditions = append(conditions, `id = $1::uuid`)
		args = append(args, projectID)
	}
	if page.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`(created_at, id) < ($%d::timestamptz, $%d::uuid)`, len(args)+1, len(args)+2))
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY created_at DESC`
	args = append(args, page.Limit+1)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := h.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list projects")
		return
	}
	defer rows.Close()

	projects := make([]project, 0)
	for rows.Next() {
		var p project
		if err := rows.Scan(&p.ID, &p.Name, &p.CreatedAt, &p.UpdatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read projects")
			return
		}
		projects = append(projects, p)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read projects")
		return
	}
	next := ""
	if len(projects) > page.Limit {
		last := projects[page.Limit-1]
		next = pagination.Encode(last.CreatedAt, last.ID)
	}
	writeJSON(w, http.StatusOK, pagination.Build(projects, page.Limit, next))
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}

	var p project
	err := h.db.QueryRow(r.Context(), `
		SELECT id::text, name, created_at::text, updated_at::text
		FROM projects WHERE id = $1::uuid`, r.PathValue("project_id")).Scan(
		&p.ID, &p.Name, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not get project")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Error: errorBody{Code: code, Message: message}})
}
