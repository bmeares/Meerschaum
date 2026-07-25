<!--
    GENERATED FILE — DO NOT EDIT.
    Regenerate with `python scripts/generate_mcp_docs.py` after changing
    `meerschaum/mcp/_tools.py`, `_resources.py`, or `_prompts.py`.
-->

# MCP Tool Reference

Every tool, resource, and prompt the [MCP server](index.md) exposes, generated
from the registry in `meerschaum.mcp`.

Over HTTP each tool requires an OAuth2 scope, and tools your token does not
cover are hidden from `tools/list` rather than refused at call time. The stdio
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
| `get_pipe_attributes` | `pipes:read` | ✅ |  | Return a pipe's full attributes: its parameters dict (columns, dtypes, tags, fetch definition, verify settings) as registered on the instance. |
| `get_pipe_data` | `pipes:read` | ✅ |  | Read rows from a pipe. |
| `get_pipe_stats` | `pipes:read` | ✅ |  | Return a pipe's shape in one call: whether its table exists, its rowcount, its newest and oldest datetime values (sync time), its verification bound time, and the column types as they actually exist in the database. |
| `list_pipes` | `pipes:read` | ✅ |  | List registered Meerschaum pipes and their keys, tags, index columns, and dtypes. |
| `read_sql` | `sql:read` | ✅ |  | Run a read-only SQL query (or read a whole table by name) through a SQL connector and return the rows. |
| `register_pipe` | `pipes:write` |  |  | Create and register a new pipe (the programmatic equivalent of 'bootstrap pipe'). |
| `sync_documents` | `pipes:write` |  |  | Insert or update rows in a pipe from a list of records you supply. |
| `sync_pipe` | `pipes:write` |  |  | Sync a pipe: fetch new data from its source connector and store it on its instance. |
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
| `pipes:delete` | `clear_pipe`, `deduplicate_pipe`, `delete_pipe` | Delete pipes' parameters and drop target tables. |
| `pipes:drop` | `drop_pipe` | Drop target tables. |
| `pipes:read` | `get_pipe_attributes`, `get_pipe_data`, `get_pipe_stats`, `list_pipes` | Read pipes' parameters and the contents of target tables. |
| `pipes:write` | `deduplicate_pipe`, `edit_pipe`, `register_pipe`, `sync_documents`, `sync_pipe`, `verify_pipe` | Update pipes' parameters and sync to target tables. |
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
| `begin` | string |  | Inclusive lower bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no lower bound. |
| `end` | string |  | Exclusive upper bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no upper bound. |
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
| `begin` | string |  | Inclusive lower bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no lower bound. |
| `end` | string |  | Exclusive upper bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no upper bound. |
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
| `begin` | string |  | Inclusive lower bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no lower bound. |
| `end` | string |  | Exclusive upper bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no upper bound. |
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
| `begin` | string |  | Inclusive lower bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no lower bound. |
| `end` | string |  | Exclusive upper bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no upper bound. |
| `params` | object |  | Additional WHERE filters, e.g. {"station": "KGMU"}. A list means IN, a leading '_' negates, and null means IS NULL. |

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

**Scope:** `pipes:write`

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

Sync a pipe: fetch new data from its source connector and store it on its instance. This is the normal incremental ingest path. Use `sync_documents` instead to insert rows you already have.

**Scope:** `pipes:write`

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string |  | Inclusive lower bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no lower bound. |
| `end` | string |  | Exclusive upper bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no upper bound. |

### `verify_pipe`

Re-sync a pipe's historical range in chunks, refetching any chunk whose local rowcount disagrees with its source. Use this to fill gaps after an outage.

**Scope:** `pipes:write`

| Argument | Type | Required | Description |
| --- | --- | :-: | --- |
| `connector_keys` | string | ✅ | The pipe's data source, e.g. 'sql:main' or 'plugin:noaa'. |
| `metric_key` | string | ✅ | The pipe's metric key, e.g. 'weather'. |
| `location_key` | string |  | The pipe's location key. Omit (or pass 'None') for a null location. |
| `instance_keys` | string |  | Where the pipe's metadata and data are stored, e.g. 'sql:main'. Omit for the server's default instance. |
| `begin` | string |  | Inclusive lower bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no lower bound. |
| `end` | string |  | Exclusive upper bound on the datetime axis (ISO 8601), or an integer as a string for integer-axis pipes. Omit for no upper bound. |


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
