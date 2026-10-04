package deliveries

import (
	"fmt"
	"net/http"

	"hookrelay/internal/pagination"
)

type attempt struct {
	ID            string `json:"id"`
	DeliveryID    string `json:"delivery_id"`
	AttemptNumber int    `json:"attempt_number"`
	ResponseCode  any    `json:"response_code"`
	Error         any    `json:"error"`
	DurationMS    any    `json:"duration_ms"`
	CreatedAt     string `json:"created_at"`
}

func (h *Handler) Attempts(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	var exists bool
	if err := h.db.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM deliveries WHERE id = $1::uuid)`, r.PathValue("delivery_id")).Scan(&exists); err != nil || !exists {
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	page, err := pagination.Parse(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	query := `SELECT id::text, delivery_id::text, attempt_number, response_code, error, duration_ms, created_at::text FROM delivery_attempts WHERE delivery_id = $1::uuid`
	args := []any{r.PathValue("delivery_id")}
	if page.Cursor != nil {
		query += fmt.Sprintf(` AND (created_at, id) < ($%d::timestamptz, $%d::uuid)`, len(args)+1, len(args)+2)
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	query += ` ORDER BY created_at DESC`
	args = append(args, page.Limit+1)
	query += fmt.Sprintf(` LIMIT $%d`, len(args))
	rows, err := h.db.Query(r.Context(), query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not list delivery attempts")
		return
	}
	defer rows.Close()
	result := make([]attempt, 0)
	for rows.Next() {
		var item attempt
		if err := rows.Scan(&item.ID, &item.DeliveryID, &item.AttemptNumber, &item.ResponseCode, &item.Error, &item.DurationMS, &item.CreatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read delivery attempts")
			return
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read delivery attempts")
		return
	}
	next := ""
	if len(result) > page.Limit {
		last := result[page.Limit-1]
		next = pagination.Encode(last.CreatedAt, last.ID)
	}
	writeJSON(w, http.StatusOK, pagination.Build(result, page.Limit, next))
}
