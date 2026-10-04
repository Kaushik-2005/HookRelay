package deliveries

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"hookrelay/internal/auth"
)

type bulkRetryRequest struct {
	ProjectID string `json:"project_id"`
	WebhookID string `json:"webhook_id"`
	Before    string `json:"before"`
	Limit     int    `json:"limit"`
}

type bulkRetryResponse struct {
	UpdatedCount int      `json:"updated_count"`
	DeliveryIDs  []string `json:"delivery_ids"`
}

func (h *Handler) BulkRetry(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	input := bulkRetryRequest{Limit: 50}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
			return
		}
		if input.Limit == 0 {
			input.Limit = 50
		}
	}
	if input.Limit < 1 || input.Limit > 100 {
		writeError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 100")
		return
	}
	if input.Before != "" {
		if _, err := time.Parse(time.RFC3339, input.Before); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "before must be an RFC3339 timestamp")
			return
		}
	}
	if projectID, ok := auth.ProjectID(r.Context()); ok {
		if input.ProjectID != "" && input.ProjectID != projectID {
			writeError(w, http.StatusForbidden, "forbidden", "API key cannot access this project")
			return
		}
		input.ProjectID = projectID
	}
	if input.ProjectID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "project_id is required")
		return
	}

	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not start bulk retry")
		return
	}
	defer tx.Rollback(r.Context())
	conditions := []string{"d.status = 'failed'", fmt.Sprintf("e.project_id = $%d::uuid", 1)}
	args := []any{input.ProjectID}
	if input.WebhookID != "" {
		conditions = append(conditions, fmt.Sprintf("d.webhook_id = $%d::uuid", len(args)+1))
		args = append(args, input.WebhookID)
	}
	if input.Before != "" {
		conditions = append(conditions, fmt.Sprintf("d.created_at < $%d::timestamptz", len(args)+1))
		args = append(args, input.Before)
	}
	limitIndex := len(args) + 1
	args = append(args, input.Limit)
	conditions2 := strings.ReplaceAll(strings.ReplaceAll(strings.Join(conditions, " AND "), "d.", "d2."), "e.", "e2.")
	query := fmt.Sprintf(`
		UPDATE deliveries d
		SET status = 'pending', next_retry_at = NOW(), lease_id = NULL,
		    lease_expires_at = NULL, updated_at = NOW()
		FROM events e
		WHERE d.event_id = e.id AND %s
		AND d.id IN (
			SELECT d2.id FROM deliveries d2 JOIN events e2 ON e2.id = d2.event_id
			WHERE %s ORDER BY d2.created_at LIMIT $%d FOR UPDATE SKIP LOCKED
		)
		RETURNING d.id::text`, strings.Join(conditions, " AND "), conditions2, limitIndex)
	rows, err := tx.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not retry deliveries")
		return
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read retried deliveries")
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not commit bulk retry")
		return
	}
	writeJSON(w, http.StatusOK, bulkRetryResponse{UpdatedCount: len(ids), DeliveryIDs: ids})
}
