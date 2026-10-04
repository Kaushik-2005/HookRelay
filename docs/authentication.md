# Authentication

Authentication is optional for local development and disabled by default.
Enable it before exposing HookRelay:

```powershell
$env:HOOKRELAY_AUTH_REQUIRED = "true"
$env:HOOKRELAY_BOOTSTRAP_KEY = "replace-with-a-long-random-secret"
```

## Bootstrap flow

The bootstrap key is sent in `X-HookRelay-Bootstrap-Key`. It is intended only
for initial setup and is not a project API key.

1. Use the bootstrap key to create the first project with `POST /projects`.
2. Use the same bootstrap key to create that project's first API key with
   `POST /projects/{project_id}/api-keys`.
3. Store the returned API key immediately; the raw key is returned only once.
4. Use the API key for subsequent project-scoped requests.

```powershell
$bootstrap = @{ "X-HookRelay-Bootstrap-Key" = $env:HOOKRELAY_BOOTSTRAP_KEY }
$project = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/projects" `
  -Headers $bootstrap -ContentType "application/json" `
  -Body '{"name":"Payment App"}'
$apiKey = Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/projects/$($project.id)/api-keys" `
  -Headers $bootstrap -ContentType "application/json" `
  -Body '{"name":"local development"}'
$headers = @{ Authorization = "Bearer $($apiKey.key)" }
```

Bootstrap access is allowed for project creation and API-key creation. Other
protected resources require `Authorization: Bearer <api-key>`.

API keys are stored as SHA-256 hashes and can be revoked with
`DELETE /api-keys/{api_key_id}`. The bootstrap secret should be long, random,
kept outside source control, and rotated through deployment configuration.

Health and the portal remain accessible without an API key. Restrict the
portal and `/metrics` at the network or reverse-proxy layer in production.
