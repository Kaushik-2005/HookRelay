package pagination

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	DefaultLimit = 25
	MaxLimit     = 100
)

type Cursor struct {
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
}

type Params struct {
	Limit  int
	Cursor *Cursor
}

type Response[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func Parse(r *http.Request) (Params, error) {
	limit := DefaultLimit
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > MaxLimit {
			return Params{}, fmt.Errorf("limit must be between 1 and %d", MaxLimit)
		}
		limit = parsed
	}
	params := Params{Limit: limit}
	if value := r.URL.Query().Get("cursor"); value != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil {
			return Params{}, fmt.Errorf("cursor is invalid")
		}
		var cursor Cursor
		if err := json.Unmarshal(decoded, &cursor); err != nil || strings.TrimSpace(cursor.CreatedAt) == "" || strings.TrimSpace(cursor.ID) == "" {
			return Params{}, fmt.Errorf("cursor is invalid")
		}
		params.Cursor = &cursor
	}
	return params, nil
}

func Encode(createdAt, id string) string {
	value, _ := json.Marshal(Cursor{CreatedAt: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(value)
}

func Build[T any](items []T, limit int, next string) Response[T] {
	if len(items) > limit {
		items = items[:limit]
	}
	return Response[T]{Data: items, NextCursor: next}
}
