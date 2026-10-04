# HookRelay benchmarks

The benchmark runner uses only the Go standard library and records every
request latency, so the same workload can be repeated on a local machine or
in CI:

```powershell
go run ./cmd/bench -url http://localhost:8080/health -requests 5000 -concurrency 50
```

For delivery benchmarks, point it at a prepared event-publishing request and
use a controllable receiver as the registered webhook target. Run separate
scenarios for:

- a fast `200` receiver;
- a receiver that sleeps longer than the worker timeout;
- `429` and `500` receivers to verify retry scheduling;
- an unavailable port to verify network-error recovery;
- a receiver that terminates the worker process after a claim, then restart
  HookRelay and verify the lease is reclaimed after five minutes (or by
  shortening the lease in a test database).

Record the command, commit, Go version, PostgreSQL version, request count,
concurrency, receiver behavior, and output. Report p50, p95, p99, throughput,
status counts, retry count, and recovery time together; latency alone does
not demonstrate delivery reliability.

The runner reports endpoint/API request latency. Delivery-specific retry and
recovery numbers come from `/deliveries/{delivery_id}/attempts` and delivery
timestamps, so they remain tied to the database source of truth.

## PostgreSQL delivery benchmark

Run the actual event-publish and delivery benchmark with PostgreSQL:

```powershell
$env:DATABASE_URL = "postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable"
$env:HOOKRELAY_BENCH_WORKER_CONCURRENCY = "1"
go test ./internal/integration -run '^$' -bench BenchmarkEventPublishAndDelivery -benchtime=100x -count=1
```

Repeat with concurrency `2`, `5`, and `10`. The benchmark publishes 100
events, starts the worker after the batch is durable to avoid a polling-phase
artifact, and waits for all deliveries. It records:

- event publish `ns/op`;
- end-to-end deliveries per second;
- end-to-end receiver-arrival p50/p95/p99;
- queue wait p50/p95/p99, measured from event creation to attempt creation;
- outbound HTTP p50/p95/p99 from `delivery_attempts.duration_ms`.

The queue-wait metric includes PostgreSQL claim and attempt-recording work.
The HTTP metric covers the outbound request/response duration. This separation
explains why a roughly 600 ms end-to-end median can coexist with a roughly
2 ms HTTP median.

### Published delivery results

Environment: October 4, 2026; Windows/amd64; Go 1.26.6; PostgreSQL 16.15 in
Docker; AMD Ryzen 7 5800H; 100 events; local `httptest` receiver returning
HTTP 200; `-benchtime=100x`.

| Worker concurrency | Publish ns/op | Deliveries/sec | End-to-end p50 | Queue p50 | Queue p95 | Queue p99 | HTTP p50 | HTTP p95 | HTTP p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5.436 ms | 78.63 | 647.023 ms | 645.1 ms | 719.7 ms | 726.0 ms | 2.000 ms | 3.000 ms | 4.170 ms |
| 2 | 5.315 ms | 84.60 | 600.572 ms | 599.1 ms | 642.4 ms | 648.1 ms | 2.000 ms | 3.000 ms | 20.040 ms |
| 5 | 5.486 ms | 82.71 | 624.751 ms | 623.0 ms | 654.3 ms | 657.0 ms | 2.000 ms | 6.800 ms | 23.020 ms |
| 10 | 5.281 ms | 87.22 | 582.138 ms | 579.8 ms | 618.3 ms | 619.3 ms | 2.000 ms | 23.050 ms | 26.010 ms |

The result shows that this workload is queue/claim bound rather than outbound
HTTP bound. Increasing worker concurrency improved throughput modestly, but did
not eliminate the database queue wait.

## Published failure measurements

Run the failure scenarios with:

```powershell
$env:DATABASE_URL = "postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable"
go test ./internal/integration -run TestFailureScenarioMeasurements -v -count=1
```

Measured October 4, 2026 with one worker and PostgreSQL 16.15:

| Scenario | First-attempt result | Time to failed state | Attempts | Retry scheduled |
|---|---|---:|---:|---|
| HTTP 429 | Response code 429 | 99 ms | 1 | Yes |
| HTTP 500 | Response code 500 | 29 ms | 1 | Yes |
| Unavailable endpoint | Network error, no response code | 27 ms | 1 | Yes |
| Slow endpoint | 10-second client timeout, no response code | 10.051 s | 1 | Yes |

These measurements verify failure classification and retry scheduling. They do
not wait through the one-minute delay for the next attempt or claim a later
retry succeeded.
