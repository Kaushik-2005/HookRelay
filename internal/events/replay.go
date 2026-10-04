package events

import (
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
)

type replayResponse struct {
	EventID      string   `json:"event_id"`
	CreatedCount int      `json:"created_count"`
	DeliveryIDs  []string `json:"delivery_ids"`
}

func (h *Handler) Replay(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is not configured")
		return
	}
	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not start replay")
		return
	}
	defer tx.Rollback(r.Context())

	var projectID, eventType string
	err = tx.QueryRow(r.Context(), `SELECT project_id::text, event_type FROM events WHERE id = $1::uuid FOR SHARE`, r.PathValue("event_id")).Scan(&projectID, &eventType)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "event not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not inspect event")
		return
	}
	rows, err := tx.Query(r.Context(), `
		INSERT INTO deliveries (event_id, webhook_id)
		SELECT $1::uuid, id FROM webhooks
		WHERE project_id = $2::uuid AND event_type = $3 AND active = TRUE
		RETURNING id::text`, r.PathValue("event_id"), projectID, eventType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create replay deliveries")
		return
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			writeError(w, http.StatusInternalServerError, "internal_error", "could not read replay deliveries")
			return
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read replay deliveries")
		return
	}
	rows.Close()
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not commit replay")
		return
	}
	writeJSON(w, http.StatusCreated, replayResponse{EventID: r.PathValue("event_id"), CreatedCount: len(ids), DeliveryIDs: ids})
}
