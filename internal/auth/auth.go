package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey string

const projectKey contextKey = "hookrelay_project_id"

type Handler struct {
	db           *pgxpool.Pool
	bootstrapKey string
}

type createRequest struct {
	Name string `json:"name"`
}

type apiKeyResponse struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Name      string `json:"name"`
	Key       string `json:"key"`
	CreatedAt string `json:"created_at"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(db *pgxpool.Pool, bootstrapKey string) *Handler {
	return &Handler{db: db, bootstrapKey: bootstrapKey}
}

func ProjectID(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(projectKey).(string)
	return value, ok
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	name := createRequest{}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&name); err != nil || strings.TrimSpace(name.Name) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	projectID := r.PathValue("project_id")
	if current, ok := ProjectID(r.Context()); ok && current != projectID && !h.bootstrapAllowed(r) {
		writeError(w, http.StatusForbidden, "forbidden", "API key cannot access this project")
		return
	}
	key, err := newKey()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not generate API key")
		return
	}
	var result apiKeyResponse
	err = h.db.QueryRow(r.Context(), `
		INSERT INTO api_keys (project_id, name, key_prefix, key_hash)
		SELECT $1::uuid, $2, $3, $4
		WHERE EXISTS (SELECT 1 FROM projects WHERE id = $1::uuid)
		RETURNING id::text, project_id::text, name, created_at::text`,
		projectID, strings.TrimSpace(name.Name), key[:16], hashKey(key)).Scan(
		&result.ID, &result.ProjectID, &result.Name, &result.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "project not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create API key")
		return
	}
	result.Key = key
	writeJSON(w, http.StatusCreated, result)
}

func (h *Handler) Revoke(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	projectID, ok := ProjectID(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "valid API key required")
		return
	}
	result, err := h.db.Exec(r.Context(), `UPDATE api_keys SET revoked_at = NOW() WHERE id = $1::uuid AND project_id = $2::uuid AND revoked_at IS NULL`, r.PathValue("api_key_id"), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not revoke API key")
		return
	}
	if result.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "not_found", "API key not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Middleware(required bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/portal") || !required {
			next.ServeHTTP(w, r)
			return
		}
		if h.bootstrapAllowed(r) && ((r.Method == http.MethodPost && r.URL.Path == "/projects") || strings.HasSuffix(r.URL.Path, "/api-keys")) {
			next.ServeHTTP(w, r)
			return
		}
		projectID, ok := h.authenticate(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid API key required")
			return
		}
		if target := h.resourceProject(r); target != "" && target != projectID {
			writeError(w, http.StatusForbidden, "forbidden", "API key cannot access this project")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), projectKey, projectID)))
	})
}

func (h *Handler) authenticate(r *http.Request) (string, bool) {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	var projectID string
	err := h.db.QueryRow(r.Context(), `UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = $1 AND revoked_at IS NULL RETURNING project_id::text`, hashKey(parts[1])).Scan(&projectID)
	return projectID, err == nil
}

func (h *Handler) resourceProject(r *http.Request) string {
	segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(segments) >= 2 && segments[0] == "projects" {
		return segments[1]
	}
	if len(segments) < 2 || h.db == nil {
		return ""
	}
	var projectID string
	var err error
	switch segments[0] {
	case "webhooks":
		err = h.db.QueryRow(r.Context(), `SELECT project_id::text FROM webhooks WHERE id = $1::uuid`, segments[1]).Scan(&projectID)
	case "events":
		err = h.db.QueryRow(r.Context(), `SELECT project_id::text FROM events WHERE id = $1::uuid`, segments[1]).Scan(&projectID)
	case "deliveries":
		err = h.db.QueryRow(r.Context(), `SELECT e.project_id::text FROM deliveries d JOIN events e ON e.id = d.event_id WHERE d.id = $1::uuid`, segments[1]).Scan(&projectID)
	case "api-keys":
		err = h.db.QueryRow(r.Context(), `SELECT project_id::text FROM api_keys WHERE id = $1::uuid`, segments[1]).Scan(&projectID)
	}
	if err != nil {
		return ""
	}
	return projectID
}

func (h *Handler) bootstrapAllowed(r *http.Request) bool {
	return h.bootstrapKey != "" && r.Header.Get("X-HookRelay-Bootstrap-Key") == h.bootstrapKey
}

func newKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "hr_live_" + base64.RawURLEncoding.EncodeToString(b), nil
}

func hashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorBody{Code: code, Message: message}})
}
