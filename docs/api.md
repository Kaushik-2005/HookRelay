# API reference

All endpoints return JSON unless noted otherwise. Collection responses use:

```json
{
  "data": [],
  "next_cursor": "..."
}
```

Pass `limit` from 1 to 100 and use `next_cursor` for the next page.

## Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Health and database status |
| GET | `/metrics` | Prometheus-compatible metrics |
| GET | `/portal` | Endpoint-management portal |
| POST | `/projects` | Create a project |
| GET | `/projects` | List projects |
| GET | `/projects/{project_id}` | Get a project |
| POST | `/projects/{project_id}/api-keys` | Create an API key |
| DELETE | `/api-keys/{api_key_id}` | Revoke an API key |
| POST | `/projects/{project_id}/webhooks` | Register a webhook |
| GET | `/projects/{project_id}/webhooks` | List webhooks |
| PATCH | `/webhooks/{webhook_id}` | Update or pause a webhook |
| DELETE | `/webhooks/{webhook_id}` | Disable a webhook |
| POST | `/webhooks/{webhook_id}/rotate-secret` | Rotate a webhook secret |
| POST | `/webhooks/{webhook_id}/verify` | Check endpoint reachability |
| POST | `/projects/{project_id}/event-types` | Register an event type |
| GET | `/projects/{project_id}/event-types` | List event types |
| PATCH | `/event-types/{event_type_id}` | Update an event type |
| POST | `/projects/{project_id}/events` | Publish an event |
| GET | `/projects/{project_id}/events` | List events |
| GET | `/events/{event_id}` | Get an event |
| POST | `/events/{event_id}/replay` | Replay an event |
| GET | `/events/{event_id}/deliveries` | List event deliveries |
| GET | `/webhooks/{webhook_id}/deliveries` | List webhook deliveries |
| GET | `/deliveries/{delivery_id}` | Get a delivery |
| GET | `/deliveries/{delivery_id}/attempts` | List delivery attempts |
| POST | `/deliveries/{delivery_id}/retry` | Retry one failed delivery |
| POST | `/deliveries/retry-failed` | Retry a filtered batch |

## Publish an event

```powershell
$eventHeaders = $headers.Clone()
$eventHeaders["Idempotency-Key"] = "payment-pay_123"
$event = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/projects/$projectId/events" `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body '{"event_type":"payment.succeeded","payload":{"payment_id":"pay_123","amount":999}}'
```

Publishing stores the event and creates matching pending deliveries in one
PostgreSQL transaction. Use the returned event ID to inspect delivery status:

```powershell
Invoke-RestMethod "http://localhost:8080/events/$($event.id)/deliveries" |
  ConvertTo-Json -Depth 5
```

## Errors

```json
{
  "error": {
    "code": "invalid_request",
    "message": "target_url is required"
  }
}
```

Common status codes are `201` for created resources, `200` for reads and
updates, `400` for invalid input, `401` for missing authentication, `403` for
project access violations, `404` for missing resources, `409` for idempotency
conflicts, and `500` for unexpected server failures.
