package observability

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Metrics struct {
	EventsPublished  atomic.Int64
	DeliveriesTotal  atomic.Int64
	DeliveryFailures atomic.Int64
	DeliveryRetries  atomic.Int64
	DeliveryDuration atomic.Int64
	DeliverySamples  atomic.Int64
	QueueDepth       atomic.Int64
}

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) ObserveDelivery(duration time.Duration, success, retry bool) {
	if m == nil {
		return
	}
	m.DeliveriesTotal.Add(1)
	m.DeliveryDuration.Add(duration.Milliseconds())
	m.DeliverySamples.Add(1)
	if !success {
		m.DeliveryFailures.Add(1)
	}
	if retry {
		m.DeliveryRetries.Add(1)
	}
}

func (m *Metrics) SetQueueDepth(depth int64) {
	if m != nil {
		m.QueueDepth.Store(depth)
	}
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		average := float64(0)
		if samples := m.DeliverySamples.Load(); samples > 0 {
			average = float64(m.DeliveryDuration.Load()) / float64(samples) / 1000
		}
		_, _ = fmt.Fprintf(w, "# TYPE hookrelay_events_published_total counter\nhookrelay_events_published_total %d\n", m.EventsPublished.Load())
		_, _ = fmt.Fprintf(w, "# TYPE hookrelay_deliveries_total counter\nhookrelay_deliveries_total %d\n", m.DeliveriesTotal.Load())
		_, _ = fmt.Fprintf(w, "# TYPE hookrelay_delivery_failures_total counter\nhookrelay_delivery_failures_total %d\n", m.DeliveryFailures.Load())
		_, _ = fmt.Fprintf(w, "# TYPE hookrelay_delivery_retries_total counter\nhookrelay_delivery_retries_total %d\n", m.DeliveryRetries.Load())
		_, _ = fmt.Fprintf(w, "# TYPE hookrelay_delivery_duration_seconds gauge\nhookrelay_delivery_duration_seconds %f\n", average)
		_, _ = fmt.Fprintf(w, "# TYPE hookrelay_queue_depth gauge\nhookrelay_queue_depth %d\n", m.QueueDepth.Load())
	})
}
