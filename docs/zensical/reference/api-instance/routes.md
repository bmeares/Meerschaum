# 🛣️ Pipelines, Runs and Actions over HTTP

Besides the pipe, user, token, plugin and job routes of Meerschaum 4 (unchanged), an API server has routes for saved [Pipelines](/reference/pipelines/), their runs and the console's background actions. Your server lists them all, with their scopes, at `/docs`.

## One response shape

These routes (`/pipelines`, `/pipelines/*`, `/engine/*`, `/execute`, `/execute/*`) always answer with a JSON object that says whether the request succeeded and what happened, plus the data:

```json
{"success": true, "message": "Found 2 runs.", "runs": [...]}
```

```json
{"success": false, "message": "Pipeline 'daily' is not registered on 'sql:main'."}
```

The status code tells the kind of failure: `400` (the request is invalid; a list of `errors` comes with it when there are several), `401` (not logged in), `403` (not allowed), `404` (not found), `409` (a conflict: it already exists, a lease is held, a scheduled job uses it), `429` (rate limited), `500` (the server failed), `501` (this server cannot do it). A `500`'s message names what failed only for administrators; everyone else gets a generic message, and the details are in the server's log.

## Saved Pipelines

| Route | Scopes | What it does |
|---|---|---|
| `GET /pipelines` | `pipes:read` | The saved Pipelines (`pipelines`, their definitions) |
| `GET /pipelines/{name}` | `pipes:read` | One Pipeline: `pipeline` (its definition), `members` (each target pipe, and whether it is registered) and `last_run` |
| `POST /pipelines/{name}/plan` | `pipes:read` | What a run would do: `plan`, `text` (the plan table) and `warnings`. Nothing moves and nothing is registered |
| `POST /pipelines/{name}/sync` | `pipes:write`, `jobs:execute` | Start a run in the background: `run_id`, `job`, `stream`, `log_stream`, `logs` |

Each takes `?instance_keys=` for a Pipeline saved on another instance. `plan` and `sync` take an optional body with the window (`{"begin": "2026-01-01", "end": "2026-02-01"}`, ISO datetimes or integers); `sync` also takes a `run_id` of your own.

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" https://mrsm.example.com/pipelines/daily/sync
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

The run is a background job, as when you press Sync on the console's Pipelines page: `stream` follows its events ([Following a run over HTTP](/reference/pipelines/visualization/#following-a-run-over-http)) and `GET /engine/runs/{run_id}` reads it when it is done. A user who is not an administrator may start a run when the server lets them run actions (`api:permissions:actions:non_admin`) and every instance the Pipeline touches is one they may use.

The registration routes the CLI and Python SDK use to save definitions are also here: `GET /pipelines/keys`, `POST /pipelines/{name}/register` (`409` if it exists), `PATCH /pipelines/{name}/edit`, `DELETE /pipelines/{name}/delete` (`409` while a scheduled job runs it), `GET /pipelines/{name}/id` and `/attributes` (`404` if it is not registered), and the leases of `/pipelines/leases`. A Pipeline named `keys` or `leases` is reachable through those routes but not through `GET /pipelines/{name}`.

## Runs

| Route | Scopes | What it does |
|---|---|---|
| `GET /engine/runs` | `pipes:read` | Runs, newest first (`runs`) |
| `GET /engine/runs/{run_id}` | `pipes:read` | One run (`run`) and each pipe's result (`nodes`) |
| `GET /engine/runs/{run_id}/events/stream` | `pipes:read` | A run's events as Server-Sent Events |

`GET /engine/runs` filters with:

- `pipeline`: the runs of a saved Pipeline;
- `pipe`: the runs that synced a pipe, as a pipe reference (`sql:main|sql:main|weather|None`);
- `status`: `success`, `failed` or `running`;
- `since`: runs started at or after an ISO datetime;
- `limit`: at most this many (default 100, at most 1000);
- `run_id`: that run and its pipes' rows;
- `instance_keys`: the instance that keeps the history (default the server's).

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://mrsm.example.com/engine/runs?pipeline=daily&status=failed&since=2026-10-01"
```

The other `/engine/*` routes carry the engine's own state for `api:` instances ([Engine State](/reference/pipelines/state/)): reads need `pipes:read`, writes `pipes:write`.

## Console actions

`POST /execute` runs an action line (`{"sysargs": ["sync", "pipes", "-c", "sql:main"]}`) as a temporary background job owned by the caller and answers with `job`, `sysargs`, `stream` (`/logs/{job}/stream`) and `logs` (`/logs/{job}`). `stream` is `null` when the server does not serve the log stream (`--no-dash`, or the `jobs` route group is off), and `logs` too without the `jobs` group. It needs `actions:execute`, `jobs:execute` and `jobs:write`; a user who is not an administrator runs only what the console offers (`GET /execute/policy`). `POST /execute/validate` answers whether the caller may run a line in the web terminal, `GET /execute/runs` lists the retained summaries of finished runs, and `POST /execute/runs/{name}/retain` keeps one as a persistent job.

## Row counts

`GET /pipes/{connector}/{metric}/{location}/rowcount` (a Meerschaum 4 route) also takes `estimate=true`, for the database's estimate of the table's size (its statistics, without counting) where it keeps one, and the exact count otherwise; and `params` as a JSON query string (`?params={"station":"KGMU"}`) for clients that cannot send a body with a `GET`.
