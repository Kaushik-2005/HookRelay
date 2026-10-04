package limits

import (
	"sync"
	"time"
)

type ProjectRateLimiter struct {
	mu      sync.Mutex
	limit   int
	windows map[string]window
}

type window struct {
	started time.Time
	count   int
}

func NewProjectRateLimiter(limit int) *ProjectRateLimiter {
	return &ProjectRateLimiter{limit: limit, windows: make(map[string]window)}
}

func (l *ProjectRateLimiter) Allow(projectID string) bool {
	if l == nil || l.limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	current, ok := l.windows[projectID]
	if !ok || now.Sub(current.started) >= time.Second {
		l.windows[projectID] = window{started: now, count: 1}
		return true
	}
	if current.count >= l.limit {
		return false
	}
	current.count++
	l.windows[projectID] = current
	return true
}

type WebhookLimiter struct {
	mu   sync.Mutex
	max  int
	sems map[string]chan struct{}
}

func NewWebhookLimiter(max int) *WebhookLimiter {
	return &WebhookLimiter{max: max, sems: make(map[string]chan struct{})}
}

func (l *WebhookLimiter) Acquire(webhookID string) func() {
	if l == nil || l.max <= 0 {
		return func() {}
	}
	l.mu.Lock()
	sem := l.sems[webhookID]
	if sem == nil {
		sem = make(chan struct{}, l.max)
		l.sems[webhookID] = sem
	}
	l.mu.Unlock()
	sem <- struct{}{}
	return func() { <-sem }
}
