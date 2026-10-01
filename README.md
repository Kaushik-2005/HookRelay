# HookRelay

HookRelay is a self-hosted webhook delivery service. Backends publish events to it, and HookRelay delivers signed webhooks, retries failures, and records delivery logs.

The core flow is: create a project, register a webhook, publish an event, and inspect its delivery status. HookRelay keeps the event and delivery history in PostgreSQL while an in-process worker handles delivery asynchronously.

## Quick start

Requirements: Go 1.23+ and Docker Desktop.

Start PostgreSQL:

```powershell
docker compose up -d postgres
$env:DATABASE_URL = "postgres://hookrelay:hookrelay@localhost:5432/hookrelay?sslmode=disable"
```

Start HookRelay in terminal 2:

```powershell
go mod download
go run ./cmd/server
```

The server listens on `:8080` by default. Check it with:

```powershell
curl http://localhost:8080/health
```

Configuration:

- `PORT` — HTTP port, default `8080`
- `DATABASE_URL` — PostgreSQL connection string; required for APIs and the worker
- `DB_MAX_CONNS` — database pool size, default `5`

## End-to-end demo

Demo flow:

1. Create a project with `POST /projects` and save its `id`.
2. Register `http://localhost:9090/webhooks` for `payment.succeeded` and save the returned `secret`.
3. Start the example receiver in another terminal with that secret:

   ```powershell
   $env:HOOKRELAY_SECRET = "<secret returned when registering the webhook>"
   go run ./examples/receiver
   ```

4. Publish an event with `POST /projects/{project_id}/events`.
5. Inspect `GET /events/{event_id}/deliveries` to see the delivered status.
6. Stop the receiver, publish another event, and inspect its delivery log as retries are scheduled.

### Copy-paste PowerShell demo

Run these commands after PostgreSQL and HookRelay are running. The first two commands create the project and webhook and save their IDs in PowerShell variables:

```powershell
$project = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/projects" `
  -ContentType "application/json" `
  -Body '{"name":"Demo Payment App"}'

$webhook = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/projects/$($project.id)/webhooks" `
  -ContentType "application/json" `
  -Body '{"event_type":"payment.succeeded","target_url":"http://localhost:9090/webhooks"}'

$project.id
$webhook.id
$webhook.secret
```

Copy the printed secret into terminal 3 and start the receiver:

```powershell
$env:HOOKRELAY_SECRET = "paste-the-secret-here"
go run ./examples/receiver
```

Return to the HookRelay terminal and publish an event:

```powershell
$event = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/projects/$($project.id)/events" `
  -ContentType "application/json" `
  -Body '{"event_type":"payment.succeeded","payload":{"payment_id":"pay_123","amount":999,"currency":"INR"}}'

$event.id
```

Wait a few seconds for the worker, then inspect the delivery:

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/events/$($event.id)/deliveries" |
  ConvertTo-Json -Depth 5
```

Expected result includes `"status": "delivered"`, `"attempt_count": 1`, and `"response_code": 200`. The receiver terminal logs the verified event body.

To observe a failure, stop the receiver, publish another event, and inspect its delivery record. Retryable failures receive a future `next_retry_at`. A failed delivery can be manually requeued with:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/deliveries/{delivery_id}/retry" |
  ConvertTo-Json -Depth 5
```

When `DATABASE_URL` is set, startup applies pending SQL files from `migrations/` and records them in `schema_migrations`.

## Projects

Create a project:

```powershell
curl -X POST http://localhost:8080/projects `
  -H "Content-Type: application/json" `
  -d '{"name":"Demo Payment App"}'
```

List projects with `GET /projects`, or fetch one with `GET /projects/{project_id}`.

Register a webhook:

```powershell
curl -X POST http://localhost:8080/projects/{project_id}/webhooks `
  -H "Content-Type: application/json" `
  -d '{"event_type":"payment.succeeded","target_url":"https://example.com/webhooks/payments"}'
```

The response includes the generated signing secret. Webhooks can be listed, updated with `PATCH /webhooks/{webhook_id}`, or disabled with `DELETE /webhooks/{webhook_id}`.

Publish an event:

```powershell
curl -X POST http://localhost:8080/projects/{project_id}/events `
  -H "Content-Type: application/json" `
  -d '{"event_type":"payment.succeeded","payload":{"payment_id":"pay_123","amount":999,"currency":"INR"}}'
```

Publishing stores the event and creates a pending delivery for every active webhook in the project matching the event type. Events can be listed with `GET /projects/{project_id}/events` or fetched with `GET /events/{event_id}`.

When PostgreSQL is configured, an in-process worker polls pending deliveries, sends signed HTTP POST requests, and records response codes or failure details. The worker uses row locking so multiple HookRelay processes do not claim the same delivery.

Retryable failures are network errors, timeouts, `409`, `429`, and `5xx` responses. They are retried with delays of 1 minute, 5 minutes, and 15 minutes, up to five attempts. Other `4xx` responses fail immediately.

Inspect delivery logs with `GET /events/{event_id}/deliveries`, `GET /webhooks/{webhook_id}/deliveries`, or `GET /deliveries/{delivery_id}`.

Manually retry a failed delivery with `POST /deliveries/{delivery_id}/retry`. Delivered deliveries cannot be retried.

## API reference

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Check server and database status |
| `POST` | `/projects` | Create a project |
| `GET` | `/projects` | List projects |
| `GET` | `/projects/{project_id}` | Get a project |
| `POST` | `/projects/{project_id}/webhooks` | Register a webhook |
| `GET` | `/projects/{project_id}/webhooks` | List project webhooks |
| `PATCH` | `/webhooks/{webhook_id}` | Update a webhook |
| `DELETE` | `/webhooks/{webhook_id}` | Disable a webhook |
| `POST` | `/projects/{project_id}/events` | Publish an event |
| `GET` | `/projects/{project_id}/events` | List project events |
| `GET` | `/events/{event_id}` | Get an event |
| `GET` | `/events/{event_id}/deliveries` | List deliveries for an event |
| `GET` | `/webhooks/{webhook_id}/deliveries` | List deliveries for a webhook |
| `GET` | `/deliveries/{delivery_id}` | Get one delivery |
| `POST` | `/deliveries/{delivery_id}/retry` | Retry a failed delivery |

API errors use this shape:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "target_url is required"
  }
}
```

## Signature verification

Every delivery includes:

```text
X-HookRelay-Event-ID
X-HookRelay-Timestamp
X-HookRelay-Signature: sha256=<hex HMAC>
```

The signature is HMAC-SHA256 over the exact raw request body:

```text
timestamp + "." + raw_body
```

Example verification in Go:

```go
func verifySignature(secret string, timestamp string, body []byte, header string) bool {
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(timestamp + "."))
    mac.Write(body)
    expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
    return hmac.Equal([]byte(expected), []byte(header))
}
```

Receivers should read and verify the raw body before unmarshaling JSON, and should reject timestamps outside their acceptable replay window.

## Idempotent event publishing

Send an optional `Idempotency-Key` header when publishing an event. Repeating the same key with the same event type and payload returns the original event without creating duplicate deliveries. Reusing the key with different request data returns `409 Conflict`.

## Tests

Run the automated checks with:

```powershell
go test ./...
```

The test suite covers URL validation, secret generation, idempotency fingerprints, HMAC signatures, and retry rules.

## Architecture summary

HookRelay is a self-hosted Go webhook delivery service for backend applications. It stores projects, subscriber webhooks, events, and delivery records in PostgreSQL. Event publishing fans out pending deliveries transactionally; an in-process worker claims them safely, signs the exact payload bytes with HMAC-SHA256, delivers them over HTTP, retries transient failures with backoff, and exposes delivery logs for debugging.

Resume bullet: Built HookRelay, a self-hosted webhook delivery service in Go that lets backend applications publish events, deliver signed webhook payloads to subscriber endpoints, retry failed deliveries with backoff, and inspect delivery logs for debugging.
