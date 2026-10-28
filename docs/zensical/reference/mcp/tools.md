<!--
    GENERATED FILE — DO NOT EDIT.
    Regenerate with `python scripts/generate_mcp_docs.py` after changing the MCP registry
    (`crates/meerschaum-server/src/mcp/`: v4's `registry.json` and v5's additions in `v5.rs`).
-->

# MCP Tool Reference

Every tool, resource, and prompt the [MCP server](index.md) exposes, generated
from the server's registry.

Over HTTP each tool requires an OAuth2 scope, and tools your token does not
cover are hidden from `tools/list` rather than refused at call time. A tool of
Pipelines, runs, jobs or connectors is also hidden when the server does not serve
the routes of its HTTP equivalent (`api:permissions:routes`). The stdio
transport grants everything, because it runs as the user who launched it.

## Tools

| Tool | Scope | Read-only | Destructive | Description |
| --- | --- | :-: | :-: | --- |
| `clear_pipe` | `pipes:delete` |  | ⚠️ | Delete rows from a pipe's table within an optional datetime range and filter, keeping the table and the pipe's registration. |
| `deduplicate_pipe` | `pipes:write`, `pipes:delete` |  | ⚠️ | Find and remove duplicate rows in a pipe by re-syncing the affected chunks. |
| `delete_pipe` | `pipes:delete` |  | ⚠️ | Delete a pipe entirely: drop its target table and remove its registration and parameters. |
| `drop_pipe` | `pipes:drop` |  | ⚠️ | Drop a pipe's target table while keeping its registration, so it can be re-synced from its source. |
| `edit_pipe` | `pipes:write` |  |  | Edit a registered pipe's parameters (columns, dtypes, tags, fetch definition, verify settings). |
| `execute_action` | `actions:execute` |  | ⚠️ | Run any Meerschaum CLI action, for anything without a dedicated tool. |
| `get_job_logs` | `jobs:read`, `logs:read` | ✅ |  | Return the most recent lines of a background job's output. |
| `get_pipe_attributes` | `pipes:read` | ✅ |  | Return a pipe's full attributes: its parameters dict (columns, dtypes, tags, fetch definition, verify settings) as registered on the instance. |
| `get_pipe_data` | `pipes:read` | ✅ |  | Read rows from a pipe. |
| `get_pipe_stats` | `pipes:read` | ✅ |  | Return a pipe's shape in one call: whether its table exists, its rowcount, its newest and oldest datetime values (sync time), its verification bound time, and the column types as they actually exist in the database. |
| `get_run` | `pipes:read` | ✅ |  | Return one run with each pipe's outcome: its action, status, window, rows inserted and updated, timing and message. |
| `list_connectors` | `connectors:read` | ✅ |  | List the connectors configured on this server (type and label, e.g. 'sql:main'). |
| `list_jobs` | `jobs:read` | ✅ |  | List the background jobs on the server with their status (running, paused, stopped), command and last result. |
| `list_pipelines` | `pipes:read` | ✅ |  | List the saved Pipelines on an instance: named groups of pipes that sync together in dependency order, with their members and latest run. |
| `list_pipes` | `pipes:read` | ✅ |  | List registered Meerschaum pipes and their keys, tags, index columns, and dtypes. |
| `list_runs` | `pipes:read` | ✅ |  | List the runs of Pipelines and dependency syncs, newest first, with their status and timing. |
| `plan_pipeline` | `pipes:read` | ✅ |  | Show what a run of a saved Pipeline would do, without moving any data: each pipe's action (sync, skip, backfill, rebuild, ...), why, and its window. |
| `read_sql` | `sql:read` | ✅ |  | Run a read-only SQL query (or read a whole table by name) through a SQL connector and return the rows. |
| `register_pipe` | `pipes:register` |  |  | Create and register a new pipe (the programmatic equivalent of 'bootstrap pipe'). |
| `sync_documents` | `pipes:write` |  |  | Insert or update rows in a pipe from a list of records you supply. |
| `sync_pipe` | `pipes:write` |  |  | Sync a pipe: fetch new data from its source connector and store it on its instance. |
| `sync_pipeline` | `pipes:write`, `jobs:execute` |  |  | Start a run of a saved Pipeline in the background: its pipes sync in dependency order. |
| `verify_pipe` | `pipes:write` |  |  | Re-sync a pipe's historical range in chunks, refetching any chunk whose local rowcount disagrees with its source. |

## Resources

| URI | Scope | Description |
| --- | --- | --- |
| `mrsm://pipes` | `pipes:read` | Every registered pipe on the default instance, with its keys, tags, index columns, and dtypes. |
| `mrsm://pipes/{connector_keys}/{metric_key}/{location_key}` | `pipes:read` | A single pipe's attributes. |
| `mrsm://actions` | `actions:execute` | The names of every available Meerschaum action, for use with the `execute_action` tool. |
| `mrsm://connectors` | `connectors:read` | The keys (type:label) of the connectors configured on this instance, e.g. 'sql:main'. |
| `mrsm://docs` | none | How to use this MCP server: Meerschaum's data model, the tools available here, and the OAuth2 scope each one requires. |

## Prompts

| Prompt | Arguments | Description |
| --- | --- | --- |
| `bootstrap_pipe` | `source`, `instance_keys` *(optional)* | Walk through designing and registering a new pipe for a described data source, choosing index columns and dtypes before writing anything. |
| `diagnose_sync_failure` | `connector_keys`, `metric_key`, `location_key` *(optional)*, `symptom` *(optional)* | Systematically diagnose why a pipe is not syncing, or is syncing the wrong rows, working from its configuration and current state rather than guesses. |
| `explain_pipe` | `connector_keys`, `metric_key`, `location_key` *(optional)* | Explain what a pipe contains and how it is configured, in terms someone unfamiliar with the instance can follow. |

## Scopes

| Scope | Unlocks | Meaning |
| --- | --- | --- |
| `actions:execute` | `execute_action` | Execute arbitrary actions. |
| `connectors:read` | `list_connectors` | Read the available connectors. |
| `jobs:execute` | `sync_pipeline` | Run jobs. |
| `jobs:read` | `get_job_logs`, `list_jobs` | Read jobs' properties |
| `logs:read` | `get_job_logs` | Read jobs' logs. |
| `pipes:delete` | `clear_pipe`, `deduplicate_pipe`, `delete_pipe` | Delete pipes' parameters and drop target tables. |
| `pipes:drop` | `drop_pipe` | Drop target tables. |
| `pipes:read` | `get_pipe_attributes`, `get_pipe_data`, `get_pipe_stats`, `get_run`, `list_pipelines`, `list_pipes`, `list_runs`, `plan_pipeline` | Read pipes' parameters and the contents of target tables. |
| `pipes:register` | `register_pipe` | Register new pipes. |
| `pipes:write` | `deduplicate_pipe`, `edit_pipe`, `sync_documents`, `sync_pipe`, `sync_pipeline`, `verify_pipe` | Update pipes' parameters and sync to target tables. |
| `sql:read` | `read_sql` | Execute read-only SQL queries against SQL connectors. |

## Tool details

### `clear_pipe`

Delete rows from a pipe's table within an optional datetime range and filter, keeping the table and the pipe's registration. Deleted rows are not recoverable — pass `begin`/`end`/`params` to scope it, and note that omitting all three deletes every row.

**Scope:** `pipes:delete`

**Annotations:** destructive

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |
| `params` | object |  | Additional WHERE filters, e.g. {"station": "KGMU"}. A list means IN, a leading '_' negates, and null means IS NULL. |

### `deduplicate_pipe`

Find and remove duplicate rows in a pipe by re-syncing the affected chunks. Rows are deleted and rewritten, so scope it with `begin`/`end`/`params` when you can.

**Scope:** `pipes:write`, `pipes:delete`

**Annotations:** destructive

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |
| `params` | object |  | Additional WHERE filters, e.g. {"station": "KGMU"}. A list means IN, a leading '_' negates, and null means IS NULL. |

### `delete_pipe`

Delete a pipe entirely: drop its target table and remove its registration and parameters. This is irreversible and cannot be re-synced afterwards.

**Scope:** `pipes:delete`

**Annotations:** destructive, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |

### `drop_pipe`

Drop a pipe's target table while keeping its registration, so it can be re-synced from its source. All stored rows are lost.

**Scope:** `pipes:drop`

**Annotations:** destructive, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |

### `edit_pipe`

Edit a registered pipe's parameters (columns, dtypes, tags, fetch definition, verify settings). By default the supplied parameters are deep-merged into the existing ones; set `replace` to true to overwrite the whole parameters dict.

**Scope:** `pipes:write`

**Annotations:** idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `parameters` | object | ✅ | Parameters to merge into (or replace) the pipe's parameters. |
| `replace` | boolean |  | Overwrite every parameter instead of deep-merging. Defaults to false. |

### `execute_action`

Run any Meerschaum CLI action, for anything without a dedicated tool. The `mrsm://actions` resource lists the available action names. This grants broad control over the Meerschaum instance and its host, so prefer a dedicated tool when one exists. Actions which execute arbitrary code are blocked by the server's denylist.

**Scope:** `actions:execute`

**Annotations:** destructive, open-world

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `action` | string | ✅ | The action to run, e.g. 'sync pipes' or 'show pipes'. The first word is the action; the rest become subactions. |
| `kwargs` | object |  | Keyword arguments for the action, e.g. {"connector_keys": ["sql:main"], "yes": true}. |

### `get_job_logs`

Return the most recent lines of a background job's output.

**Scope:** `jobs:read`, `logs:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `name` | string | ✅ | The job's name, from `list_jobs` or `sync_pipeline`. |
| `lines` | integer |  | How many of the last lines to return (default 200, at most 5000). |

### `get_pipe_attributes`

Return a pipe's full attributes: its parameters dict (columns, dtypes, tags, fetch definition, verify settings) as registered on the instance.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |

### `get_pipe_data`

Read rows from a pipe. Narrow the result with `begin`/`end` (the datetime axis) and `params` (a WHERE filter) — on a datetime-axis pipe a time window is much cheaper than paging. Set `backtrack_minutes` to read the most recent rows relative to the pipe's sync time without knowing its bounds. Pass the returned `next_cursor` back as `cursor` for the next page.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |
| `params` | object |  | Additional WHERE filters, e.g. {"station": "KGMU"}. A list means IN, a leading '_' negates, and null means IS NULL. |
| `select_columns` | array |  | Only return these columns. Omit for all. |
| `omit_columns` | array |  | Return every column except these. |
| `backtrack_minutes` | integer |  | Read the rows within this many minutes before the pipe's newest sync time. Mutually exclusive with `begin`/`end`. |
| `order` | `asc` \| `desc` |  | Sort direction on the datetime axis. Defaults to 'asc'. |
| `limit` | integer |  | Maximum rows per page (default 200). |
| `cursor` | string |  | Opaque cursor from a previous call's `next_cursor`. Omit to start from the beginning. |

### `get_pipe_stats`

Return a pipe's shape in one call: whether its table exists, its rowcount, its newest and oldest datetime values (sync time), its verification bound time, and the column types as they actually exist in the database. Prefer this over several separate lookups.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |
| `params` | object |  | Additional WHERE filters, e.g. {"station": "KGMU"}. A list means IN, a leading '_' negates, and null means IS NULL. |

### `get_run`

Return one run with each pipe's outcome: its action, status, window, rows inserted and updated, timing and message.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `run_id` | string | ✅ | The run's ID, from `list_runs` or `sync_pipeline`. |
| `instance_keys` | string |  | Where the Pipeline (or run) is kept, e.g. 'sql:main'. Omit for the server's default instance. |

### `list_connectors`

List the connectors configured on this server (type and label, e.g. 'sql:main'). Credentials are never included.

**Scope:** `connectors:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `type` | string |  | Only connectors of this type, e.g. 'sql'. |

### `list_jobs`

List the background jobs on the server with their status (running, paused, stopped), command and last result. Credentials in their environment are masked.

**Scope:** `jobs:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `status` | `running` \| `paused` \| `stopped` |  | Only jobs with this status. |

### `list_pipelines`

List the saved Pipelines on an instance: named groups of pipes that sync together in dependency order, with their members and latest run. Use `plan_pipeline` to see what a run would do and `sync_pipeline` to start one.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `instance_keys` | string |  | Where the Pipeline (or run) is kept, e.g. 'sql:main'. Omit for the server's default instance. |

### `list_pipes`

List registered Meerschaum pipes and their keys, tags, index columns, and dtypes. Start here to discover what data is available. Results are paginated — pass the returned `next_cursor` back as `cursor` to continue.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | array |  | Only include pipes with these connector keys. Omit for all. |
| `metric_keys` | array |  | Only include pipes with these metric keys. Omit for all. |
| `location_keys` | array |  | Only include pipes with these location keys. Use 'None' to match a null location. |
| `tags` | array |  | Only include pipes carrying at least one of these tags. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `cursor` | string |  | Opaque cursor from a previous call's `next_cursor`. Omit to start from the beginning. |

### `list_runs`

List the runs of Pipelines and dependency syncs, newest first, with their status and timing. Filter by `pipeline`, `pipe` (a pipe reference 'instance|connector|metric|location'), `status` and `since`.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `instance_keys` | string |  | Where the Pipeline (or run) is kept, e.g. 'sql:main'. Omit for the server's default instance. |
| `pipeline` | string |  | Only runs of this saved Pipeline. |
| `pipe` | string |  | Only runs that synced this pipe, e.g. 'sql:main|sql:main|weather|None'. |
| `status` | `success` \| `failed` \| `running` |  | Only runs with this status. |
| `since` | string |  | Only runs started at or after this ISO 8601 datetime. |
| `limit` | integer |  | At most this many runs (default 20). |

### `plan_pipeline`

Show what a run of a saved Pipeline would do, without moving any data: each pipe's action (sync, skip, backfill, rebuild, ...), why, and its window. Nothing is registered or written.

**Scope:** `pipes:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `name` | string | ✅ | The Pipeline's name, e.g. 'daily'. |
| `instance_keys` | string |  | Where the Pipeline (or run) is kept, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |

### `read_sql`

Run a read-only SQL query (or read a whole table by name) through a SQL connector and return the rows. Only single read-only statements are accepted: anything that writes, or more than one statement, is refused. Read the `mrsm://connectors` resource for the available connector keys.

**Scope:** `sql:read`

**Annotations:** read-only, idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | SQL connector keys, e.g. 'sql:main'. |
| `query` | string | ✅ | A single read-only SQL statement, or a table name. |
| `limit` | integer |  | Maximum rows per page (default 200). |
| `cursor` | string |  | Opaque cursor from a previous call's `next_cursor`. Omit to start from the beginning. |

### `register_pipe`

Create and register a new pipe (the programmatic equivalent of 'bootstrap pipe'). Declare its index columns (`datetime` drives incremental sync, `id`/`primary` give uniqueness) and optional dtypes and tags. For a SQL-fetch pipe, set `parameters.fetch.definition` to the source query.

**Scope:** `pipes:register`

**Annotations:** idempotent

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `columns` | object |  | Index columns by role, e.g. {"datetime": "ts", "id": "station", "primary": "id"}. |
| `dtypes` | object |  | Explicit column dtypes, e.g. {"val": "float64", "attrs": "json"}. |
| `tags` | array |  | Tags for grouping and selecting this pipe. |
| `parameters` | object |  | The full parameters dict, merged with `columns`/`dtypes`/`tags`. Common keys: `fetch.definition` (source SQL), `fetch.backtrack_minutes`, `upsert`, `autotime`, `autoincrement`, `verify.chunk_minutes`. |

### `sync_documents`

Insert or update rows in a pipe from a list of records you supply. No source connector is consulted — Meerschaum filters duplicates and upserts according to the pipe's configuration.

**Scope:** `pipes:write`

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `documents` | array | ✅ | Rows as dicts, e.g. [{"ts": "2024-01-01", "station": "KGMU", "val": 1.1}]. |

### `sync_pipe`

Sync a pipe: fetch new data from its source connector and store it on its instance. This is the normal incremental ingest path. Use `sync_documents` instead to insert rows you already have. Pass `parents` or `children` to also sync the pipe's upstream or downstream pipes in dependency order.

**Scope:** `pipes:write`

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |
| `parents` | boolean \| integer |  | Also sync the pipe's upstream pipes first, in dependency order: true for every generation, or a number of generations. |
| `children` | boolean \| integer |  | Also sync the pipe's downstream pipes after it, in dependency order: true for every generation, or a number of generations. |

### `sync_pipeline`

Start a run of a saved Pipeline in the background: its pipes sync in dependency order. Returns the `run_id` (follow it with `get_run`) and the background `job` (read its output with `get_job_logs`).

**Scope:** `pipes:write`, `jobs:execute`

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `name` | string | ✅ | The Pipeline's name, e.g. 'daily'. |
| `instance_keys` | string |  | Where the Pipeline (or run) is kept, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |

### `verify_pipe`

Re-sync a pipe's historical range in chunks, refetching any chunk whose local rowcount disagrees with its source. Use this to fill gaps after an outage.

**Scope:** `pipes:write`

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string \| integer |  | Inclusive lower bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no lower bound. |
| `end` | string \| integer |  | Exclusive upper bound on the datetime axis: an ISO 8601 datetime, or an integer for integer-axis pipes. Omit for no upper bound. |


## Annotations

Every tool carries the MCP annotations a client uses to decide whether to prompt
before calling:

| Annotation | Meaning |
| --- | --- |
| `readOnlyHint` | The tool never modifies anything. |
| `destructiveHint` | The tool deletes or overwrites data. |
| `idempotentHint` | Calling it twice with the same arguments has the same effect as once. |
| `openWorldHint` | The tool can reach beyond this Meerschaum instance. |

## Pagination

`list_pipes`, `get_pipe_data`, and `read_sql` return a `next_cursor`. Pass it
back as `cursor` for the next page; a `null` cursor means there is nothing more.

On a pipe with a datetime axis, narrowing with `begin` and `end` is far cheaper
than paging — cursors re-read the rows they skip.
