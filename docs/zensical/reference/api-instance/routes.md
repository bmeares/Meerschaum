# 🛣️ Pipelines, Access and Actions over HTTP

In addition to core pipe, user, token, plugin, and job routes, the API server exposes endpoints for saved [Pipelines](/reference/pipelines/), engine runs, [teams and pipe access](/reference/pipes/access-and-locks/), and background actions. Browse the live OpenAPI spec and scopes at `/docs`.

## Response shape

Routes under `/pipelines`, `/engine`, `/execute`, `/teams`, and the access/permission endpoints below return a consistent JSON envelope:

```json
{"success": true, "message": "Found 2 runs.", "runs": [...]}
```

```json
{"success": false, "message": "Pipeline 'daily' is not registered on 'sql:main'."}
```

| Status | Meaning |
|---|---|
| `400` | Invalid request (includes `errors` list when multiple checks fail). |
| `401` / `403` | Not signed in / missing required scope or access. |
| `404` | Not found or hidden from the caller. |
| `409` | Conflict (already exists, lease held, locked, or in use by a scheduled job). |
| `429` | Rate limited. |
| `500` / `501` | Server error (details shown to admins only) / unsupported on this server. |

## Saved Pipelines

| Route | Scopes | What it does |
|---|---|---|
| `GET /pipelines` | `pipes:read` | List visible saved Pipelines. |
| `GET /pipelines/{name}` | `pipes:read` | Read a Pipeline's definition, members, and `last_run`. |
| `POST /pipelines/{name}/plan` | `pipes:read` | Dry-run plan (`plan`, `text`, `warnings`); modifies nothing. |
| `POST /pipelines/{name}/sync` | `pipes:write`, `jobs:execute` | Start a background run (`run_id`, `job`, `stream`, `log_stream`, `logs`). |
| `POST /pipelines/{name}/rename` | `pipes:read`, `pipes:write`, `pipes:register`, `pipes:delete` | Rename a live Pipeline while keeping its ID and access. |

Pass `?instance_keys=sql:other` to target another instance. `plan` and `sync` accept an optional JSON body (`{"begin": "2026-01-01", "end": "2026-02-01"}`, ISO datetimes or integers; `sync` also accepts a custom `run_id`):

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  https://mrsm.example.com/pipelines/daily/sync
```

```json
{
  "success": true,
  "message": "Started Pipeline 'daily' (run '2026-10-27T05-00-00Z-api-1a2b3c4d5e6f') as job 'execute-1a2b3c4d'.",
  "run_id": "2026-10-27T05-00-00Z-api-1a2b3c4d5e6f",
  "job": "execute-1a2b3c4d",
  "stream": "/engine/runs/2026-10-27T05-00-00Z-api-1a2b3c4d5e6f/events/stream?wait=30",
  "log_stream": "/logs/execute-1a2b3c4d/stream",
  "logs": "/logs/execute-1a2b3c4d"
}
```

Follow live events via `stream` ([Following a run over HTTP](/reference/pipelines/visualization/#following-a-run-over-http)) and read final results from `GET /engine/runs/{run_id}`. Sync-locked members are skipped using their existing data; `--secure` restricts remote mutations to admins.

??? example "Rename a Pipeline & SDK registration routes"
    Send `{"new_name": "evening"}` to `POST /pipelines/daily/rename` (optionally with `expected_revision` or `expected_id` to reject stale updates).

    The CLI and Python SDK also use `GET /pipelines/keys`, `POST /pipelines/{name}/register`, `PATCH /pipelines/{name}/edit`, `DELETE /pipelines/{name}/delete`, `GET /pipelines/{name}/id`, `GET /pipelines/{name}/attributes`, and `/pipelines/leases`.

## Runs

| Route | Scopes | What it does |
|---|---|---|
| `GET /engine/runs` | `pipes:read` | List runs, newest first. |
| `GET /engine/runs/{run_id}` | `pipes:read` | Fetch one run and every node's result. |
| `GET /engine/runs/{run_id}/events/stream` | `pipes:read` | Stream run events over Server-Sent Events (SSE). |
| `POST /engine/run-generations` | `pipes:read` | Verify whether retained run IDs match current registrations and access. |

Filter `GET /engine/runs` with query parameters (`pipeline`, `pipe`, `status`, `since`, `limit`, `run_id`, `instance_keys`):

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://mrsm.example.com/engine/runs?pipeline=daily&status=failed&since=2026-10-01"
```

??? info "Checking run generations (`POST /engine/run-generations`)"
    Send up to 256 run IDs to verify they still belong to current visible registrations:

    ```json
    {"run_ids": ["daily-2026-10-27T05-00-00Z-1a2b3c4d"]}
    ```

    The `generations` map returns `true` or `false` per ID (hidden, missing, or stale runs return `false`).

## Console actions & jobs

Run a Meerschaum command asynchronously with `POST /execute` (requires `actions:execute`, `jobs:execute`, and `jobs:write`):

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sysargs": ["sync", "pipes", "-c", "sql:main"]}' \
  https://mrsm.example.com/execute
```

| Route | Purpose |
|---|---|
| `POST /execute` | Launch an action as a temporary job owned by the caller; returns `job`, `stream`, and `logs`. |
| `GET /execute/policy` | Inspect which actions the caller is allowed to run. |
| `POST /execute/validate` | Check whether a command line is allowed in the web terminal. |
| `GET /execute/runs` | List retained summaries of finished console runs. |
| `POST /execute/runs/{name}/retain` | Promote a temporary run into a persistent job. |

Every job started through the API records the caller as `owner` and their primary team as `team`. `/jobs/*` and `/logs/*` return `404` for jobs outside the caller's visibility—see [Who Sees a Job](/reference/background-jobs/access/).

## Teams, permissions and pipe access

| Route | What it does |
|---|---|
| `GET /teams`, `GET /teams/{name}` | List teams or inspect members and managers (`?counts=true` adds pipe/job counts). |
| `POST /teams/{name}/register` | Create a team (admin). |
| `PATCH /teams/{name}/edit` | Edit members (team manager or admin) and managers/attributes (admin). |
| `POST /teams/{name}/rename`, `DELETE /teams/{name}/delete` | Rename or delete a team and update its pipes and jobs (admin). |
| `GET /users/permissions`, `GET /users/{username}/permissions` | Read role, effective permissions, teams, and primary team. |
| `PATCH /users/{username}/permissions` | Update role or explicit permissions (admin). |
| `GET /pipes/{connector}/{metric}/{location}/access` | Read a pipe's access, locks, and caller's allowed operations. |
| `PATCH /pipes/{connector}/{metric}/{location}/access` | Update owner, team, visibility, or edit mode. |
| `POST /pipes/{connector}/{metric}/{location}/lock`, `POST .../unlock` | Add or remove locks, e.g. `{"locks": ["sync", "drop"]}`. |

Pipe access and lock edits require the owner, a team manager, or an admin (only admins can assign another owner). See [Roles and teams](permissions.md#roles-and-teams).

## Row counts

`GET /pipes/{connector}/{metric}/{location}/rowcount` accepts:

- `estimate=true` — use fast database table statistics instead of a full `COUNT(*)` where supported.
- `params={"station":"KGMU"}` — pass filter `params` as a JSON query string when sending a `GET` without a body.
