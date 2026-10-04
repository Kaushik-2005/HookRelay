# HookRelay

[![CI](https://github.com/Kaushik-2005/HookRelay/actions/workflows/ci.yml/badge.svg)](https://github.com/Kaushik-2005/HookRelay/actions/workflows/ci.yml)

HookRelay is a self-hosted webhook delivery service for backend applications.

It accepts events, fans them out to matching subscriber endpoints, signs the exact request body with HMAC-SHA256, retries transient failures, and stores delivery history for debugging.

## Features

- Durable event publishing backed by PostgreSQL
- At-least-once delivery with crash recovery
- Automatic retries for transient failures
- HMAC-SHA256 signed webhook requests
- Delivery attempts, response codes, and retry history
- SSRF protection for outbound webhook targets

## Architecture

    Backend application
            |
            | publish event
            v
    +--------------------+       +--------------------+
    | HookRelay API      |------>| PostgreSQL         |
    | projects/events    |       | source of truth    |
    | webhooks/logs      |       | events + deliveries|
    +--------------------+       +<-------------------+
                                      ^
                                      | claim pending rows,
                                      | record attempts/results
                                +-----+--------------+
                                | Delivery worker   |
                                | claim -> send     |
                                | retry -> record   |
                                +-----+--------------+
                                      |
                                      v
                              Signed subscriber HTTP endpoints

The API and worker both use PostgreSQL. The worker claims pending rows with
database locks and leases; it does not receive deliveries directly from the
API process. They can run together or as separate processes.

## Contents

- Features
- Architecture
- Quick start
- Configuration
- End-to-end demo
- API essentials
- Delivery behavior
- Reliability guarantees
- Security
- Performance and testing
- Documentation
- Limitations
- Development
- Operations

## Quick start

Requirements: Go 1.23 or newer and Docker Desktop.

Start PostgreSQL:

    docker compose up -d postgres

Set the database URL in the terminal that will run HookRelay:

    $env:DATABASE_URL = "postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable"

Start the server:

    go mod download
    go run ./cmd/server

Check health:

    curl http://localhost:8080/health

Expected response:

    {"status":"ok","database":"connected"}

Migrations run automatically at startup and are recorded in the schema_migrations table.

Open the endpoint portal at http://localhost:8080/portal.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| PORT | 8080 | HTTP port |
| DATABASE_URL | empty | PostgreSQL connection string |
| DB_MAX_CONNS | 5 | Database pool size |
| HOOKRELAY_AUTH_REQUIRED | false | Require project API keys |
| HOOKRELAY_BOOTSTRAP_KEY | empty | Bootstrap secret for initial project/API-key setup |
| HOOKRELAY_RATE_LIMIT_PER_SECOND | 0 | Event publishes per project per second; zero disables the limit |
| HOOKRELAY_MAX_PENDING_DELIVERIES | 10000 | Project queue limit; zero disables the limit |
| HOOKRELAY_MAX_CONCURRENT_DELIVERIES | 10 | Worker concurrency per webhook |
| HOOKRELAY_ALLOW_PRIVATE_NETWORKS | false | Allow local/private webhook targets; use only for local development |
| HOOKRELAY_WORKER_ENABLED | true | Run the delivery worker in this process; set false for API-only mode |

For production, enable authentication, use a long random bootstrap key, run behind TLS, and restrict access to the metrics endpoint.

## Documentation

- [API reference](docs/api.md)
- [Authentication and bootstrap flow](docs/authentication.md)
- [Benchmark methodology and published results](benchmarks/README.md)
- [Continuous integration workflow](.github/workflows/ci.yml)

## End-to-end demo

The demo uses development mode with authentication disabled. For an
authenticated setup, enable auth and use the bootstrap key for project and
initial API-key creation; use the resulting `$headers` API-key header for
webhook, event, and delivery-management requests. See the
[authentication guide](docs/authentication.md).

Use four terminals.

Terminal 1:

    docker compose up -d postgres

Terminal 2:

    $env:DATABASE_URL = "postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable"
    $env:HOOKRELAY_ALLOW_PRIVATE_NETWORKS = "true"
    go run ./cmd/server

Terminal 3, create a project and webhook:

    $project = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/projects" -ContentType "application/json" -Body '{"name":"Demo Payment App"}'
    $webhook = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/projects/$($project.id)/webhooks" -ContentType "application/json" -Body '{"event_type":"payment.succeeded","target_url":"http://localhost:9090/webhooks"}'
    $project.id
    $webhook.id
    $webhook.secret

Terminal 4, signed receiver:

    $env:HOOKRELAY_SECRET = "paste-the-webhook-secret-here"
    go run ./examples/receiver

Back in terminal 3, publish and inspect:

    $event = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/projects/$($project.id)/events" -ContentType "application/json" -Body '{"event_type":"payment.succeeded","payload":{"payment_id":"pay_123","amount":999,"currency":"INR"}}'
    Invoke-RestMethod -Uri "http://localhost:8080/events/$($event.id)/deliveries" | ConvertTo-Json -Depth 5

A successful delivery has status delivered, attempt_count 1, and response_code 200.

## API essentials

The main workflow uses `POST /projects`, `POST /projects/{project_id}/webhooks`,
`POST /projects/{project_id}/events`, and
`GET /events/{event_id}/deliveries`.

See the [full API reference](docs/api.md) for all endpoints, pagination,
replays, retries, delivery attempts, and management operations.

## How it works

The worker claims due deliveries with PostgreSQL row locks and a five-minute lease. A lease ID is checked when a result is recorded, so a worker whose claim expired cannot overwrite the result of a newer worker. The worker sends signed HTTP POST requests and records response codes, errors, attempt duration, and retry state.

Delivery lifecycle:

    event accepted
        -> pending delivery rows created in the same transaction
        -> worker claims a row with an expiring lease
        -> delivering
        -> delivered
           or failed -> retry due -> delivering

HookRelay provides at-least-once delivery. If a process or network fails after the receiver accepts a request but before HookRelay records success, the request can be sent again. Receivers should deduplicate by X-HookRelay-Event-ID. PostgreSQL preserves events and delivery history; a crashed worker's lease is reclaimed after it expires. Graceful shutdown stops new HTTP work and lets in-flight API requests finish within the shutdown timeout.

Retryable failures are network errors, timeouts, HTTP 408, 409, 429, and 5xx responses. Other 4xx responses fail immediately. Manual retry and event replay preserve previous delivery history.

Retry schedule:

| Attempt | Timing |
|---|---|
| 1 | Immediate |
| 2 | After 1 minute |
| 3 | After 5 minutes |
| 4 | After 15 minutes |
| 5 | Final attempt, after 15 minutes; if it fails, permanent failure |

Attempt five is sent and can succeed. Permanent failure is recorded only when
that fifth attempt also fails; HookRelay does not schedule a sixth attempt.
Each delay is measured from the preceding failed attempt, not from the
original event creation time.

## Reliability guarantees

Events and their initial delivery rows are committed in one PostgreSQL
transaction. A successful event response therefore means the event is stored
with its matching delivery work, not merely accepted in memory.

Event publishing supports `Idempotency-Key`:

- Repeating the same key and request returns the original event.
- Reusing a key with a different event type or payload returns `409 Conflict`.
- Concurrent requests using the same key are resolved through the PostgreSQL
  unique index rather than creating duplicate events.

Worker recovery behavior:

- Claims use `FOR UPDATE SKIP LOCKED` and a five-minute lease.
- A worker crash leaves the delivery in `delivering`; another worker can
  reclaim it after the lease expires.
- Completion requires the current lease ID, preventing an old worker from
  overwriting a newer attempt.
- A request may be delivered more than once if the receiver accepts it before
  HookRelay records the result. Consumers must be idempotent.
- Delivery attempts, response codes, errors, durations, and retry state remain
  queryable after recovery.

The server handles SIGINT and SIGTERM, stops claiming new deliveries, and
allows already-claimed deliveries to finish within the bounded outbound
request context. Outbound webhook requests have a 10-second HTTP client
timeout; if it expires, the attempt is recorded as a network failure, the
delivery becomes failed, and a retry is scheduled when attempts remain. HTTP
graceful shutdown has a 15-second deadline and waits for the worker before
closing the database pool. HTTP read, write, idle, and header timeouts are
also configured to avoid indefinitely held connections.

## Security

Every webhook request includes:

    X-HookRelay-Event-ID
    X-HookRelay-Delivery-ID
    X-HookRelay-Timestamp
    X-HookRelay-Signature: sha256=<hex HMAC>

The signing input is:

    timestamp + "." + raw_body

Receivers must verify the raw body before JSON unmarshaling and should reject old timestamps to reduce replay risk. Webhook secrets are returned during creation or rotation and should be stored securely.

Webhook requests do not follow redirects. DNS names are resolved again at connection time, and resolved loopback, private, link-local, multicast, unspecified, and reserved addresses are blocked by default to reduce SSRF and DNS-rebinding risk. Local receivers such as `localhost:9090` require `HOOKRELAY_ALLOW_PRIVATE_NETWORKS=true`; do not enable that setting for an internet-facing deployment. Custom headers cannot replace the Host, Content-Length, connection, or HookRelay signature headers.

Deduplication guidance:

- `X-HookRelay-Event-ID` identifies the logical event. Every subscriber
  delivery for that event and every retry of the same delivery uses the same
  event ID.
- `X-HookRelay-Delivery-ID` identifies one delivery record for one webhook.
  Retries reuse that delivery ID, while a manual replay creates a new
  delivery ID for the same event.
- Deduplicate by event ID when an event should be processed once globally.
  Deduplicate by delivery ID when each subscriber delivery should be handled
  once while still allowing a manual replay to be processed.

Therefore, `X-HookRelay-Event-ID` alone is not sufficient to distinguish
fan-out deliveries or a manual replay. Receivers should choose the key that
matches their processing semantics and still make handlers idempotent.

API errors use:

    {
      "error": {
        "code": "invalid_request",
        "message": "target_url is required"
      }
    }

## Observability

GET /metrics exposes event, delivery, failure, retry, duration, and queue-depth metrics. Request logs are emitted as JSON with method, path, status, and duration.

## Performance and testing

Run the reproducible standard-library benchmark runner:

    go run ./cmd/bench -url http://localhost:8080/health -requests 5000 -concurrency 50

It reports throughput, errors, status counts, and p50/p95/p99 latency. Use a controlled receiver plus the delivery and attempt endpoints for slow, 429, 500, unavailable-endpoint, and worker-restart scenarios. See `benchmarks/README.md` for the measurement protocol. Results are environment-dependent, so record the command and runtime versions with each run.

With PostgreSQL running, execute the database-backed reliability tests and
delivery benchmark:

    $env:DATABASE_URL = "postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable"
    go test ./internal/integration -v -count=1
    $env:HOOKRELAY_BENCH_WORKER_CONCURRENCY = "1"
    go test ./internal/integration -run '^$' -bench BenchmarkEventPublishAndDelivery -benchtime=100x -count=1

Repeat the benchmark at worker concurrency `2`, `5`, and `10`. See
[benchmarks/README.md](benchmarks/README.md) for the exact test matrix,
published results, queue-wait analysis, and failure scenarios.

### Published performance summary

The latest PostgreSQL-backed run achieved 78.63-87.22 deliveries/second at
worker concurrency 1-10. End-to-end p50 latency was 582-647 ms, but the
recorded HTTP p50 was 2 ms; most latency was queue/database waiting. Detailed
tables, methodology, and failure-scenario results are in
[benchmarks/README.md](benchmarks/README.md). These are local development
measurements, not production capacity guarantees.

### Current benchmark result

Smoke result on October 4, 2026, Windows, Go local server without PostgreSQL:
`1000` health requests at concurrency `25` completed with `0` errors in
`175 ms` (`5700.09 req/s`); p50 `3.514 ms`, p95 `5.997 ms`, p99 `26.112 ms`,
all responses HTTP 200.

Important limitation: these results measure HTTP health-check performance, not
webhook delivery performance. They should not be presented as HookRelay event
publishing or delivery throughput. Actual delivery benchmarks must include
event creation, PostgreSQL writes, delivery claims, signed outbound requests,
retries, slow receivers, HTTP 429/500 responses, unavailable endpoints, and
worker recovery.

## Limitations

- Delivery is at least once, not exactly once. A receiver may receive a
  request again if HookRelay loses the result after the receiver accepted it.
- Receivers are responsible for idempotent processing and should choose event
  ID or delivery ID deduplication according to their semantics.
- Local benchmark results depend on hardware, PostgreSQL, network conditions,
  and receiver behavior. They are not production capacity guarantees.
- The default worker polls PostgreSQL every five seconds. Event publishing is
  durable immediately, while delivery pickup latency depends on polling and
  current queue load.

## Production deployment checklist

Use this as a deployment review checklist; it is not a claim that every
operational control is provided by the application:

- [ ] Enable API authentication and configure a strong bootstrap secret
- [ ] Deploy behind HTTPS
- [ ] Keep private-network webhook targets disabled
- [ ] Restrict access to `/metrics` and `/portal`
- [ ] Configure PostgreSQL backups and connection limits
- [ ] Define monitoring and alerts for failed deliveries and queue depth
- [ ] Test recovery and graceful shutdown in the deployment environment

## Operations

If health reports database not configured, set DATABASE_URL before starting the server.

If port 8080 is occupied, find the listener:

    Get-NetTCPConnection -LocalPort 8080 -State Listen

Or use another port:

    $env:PORT = "8081"
    go run ./cmd/server

If a webhook remains pending, check the worker log, receiver process, delivery record, and delivery-attempt endpoint.

## Development

Run tests and static checks:

    go test ./...
    go vet ./...

The test suite includes webhook URL validation, redirect blocking, private-IP
blocking, HMAC signing, retry classification, retry timing, and idempotency
fingerprint checks. Database-backed recovery scenarios should be run with the
Docker PostgreSQL service and the benchmark matrix above.

Worker shutdown behavior is also covered by the implementation: shutdown stops
new claims, lets already-claimed deliveries finish within the bounded delivery
context, waits for the worker, and only then closes the database pool.

GitHub Actions runs `go test ./...` and `go vet ./...` on pushes and pull
requests. PostgreSQL integration tests require a database URL and are run
explicitly with the command in the Benchmarks section.

PostgreSQL is the source of truth. The API and worker share internal packages,
and can run together with the default configuration or as separate processes:

    $env:HOOKRELAY_WORKER_ENABLED = "false"
    go run ./cmd/server

    go run ./cmd/worker
