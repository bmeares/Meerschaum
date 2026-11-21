# Troubleshooting

A symptom-driven guide to common Meerschaum issues (**Symptom → Cause → Fix**). For terminology, see the [glossary](glossary.md).

!!! tip "First step: run `mrsm doctor` or add `--debug`"
    `mrsm doctor` checks your installation in about a second without modifying anything. Adding `--debug` to any command prints resolved SQL, sync windows, and dtypes to stderr. See [Debug output](#debug-output).

---

## Start here: `mrsm doctor`

`mrsm doctor` is a **read-only** health check—it never installs packages, writes default config files, or imports plugin code (opening an existing SQLite database may create `-wal`/`-shm` files). Each check reports **PASS**, **WARN**, **FAIL**, or **SKIP** with an actionable fix, and the command exits `1` on any FAIL (including with `--nopretty`).

```bash
mrsm doctor                  # human-readable table with fixes
mrsm doctor -i sql:main      # check a specific instance
mrsm doctor --nopretty       # JSON document for scripts or bug reports (summary.ok / summary.fail)
mrsm doctor --debug          # include per-check timings
```

| Group | What it checks |
|---|---|
| `install` | Version, runtime mode (extension vs binary), `MRSM_PYTHON` worker interpreter, and mismatched/stale `meerschaum` copies (e.g. a v4 copy in `~/.local`). |
| `paths` | Root, config, and plugins directories are writable; free disk space and inodes on the root and temp filesystems (a full `/tmp` breaks workers). |
| `config` | Config files parse cleanly, no unknown or removed v4 keys (`system:experimental:join_fetch`), active `MRSM_<TYPE>_<LABEL>` names (never values), and default instance. |
| `instance` | Connectivity (5 s limit), server version, and `mrsm_*` tables on `sql:` instances (or `/healthcheck` and `/version` on `api:`). Skips live queries on Python DuckDB and missing SQLite files to stay read-only. |
| `drivers` | Python DuckDB metadata (without importing it) and native `libduckdb` / Oracle client availability (`mrsm show drivers`). Never downloads drivers. |
| `extras` | Which optional extras (`pandas`, `polars`, `geo`, `sql`, `api`, `dash`, `shell`) are importable, warning if an installed plugin needs a missing one. |
| `plugins` | Cached plugin manifest freshness (`mrsm reload` rebuilds it), failed imports, and venvs built with a different Python interpreter. |
| `jobs` | Running, paused, stopped, or recently failed jobs, plus stale PID files. |
| `stack` | Container state and health for each Docker stack service (if configured). |
| `extensions` | PostgreSQL extensions vs versions shipped by the server (see [Stack upgrades](/reference/stack/upgrades/)). |

!!! note "SDK usage"
    Importing `meerschaum.actions` discovers plugins before any action runs. Use `mrsm.entry(['doctor', ...])` from Python to stay strictly read-only.

---

## Syncing

### Sync succeeds but no rows land

**Symptom:** `sync pipes` succeeds (`Inserted 0 rows, updated 0 rows`), but the table stays empty or unchanged.

**Cause:** Usually one of three things:

1. **Deduplication:** `pipe.filter_existing(df)` dropped incoming rows that already match stored rows.
2. **Sync window:** With `begin=''` (default), Meerschaum only fetches from `sync_time - fetch:backtrack_minutes`.
3. **Late-arriving edits:** Source updates older than `backtrack_minutes` fall outside the fetch window.

**Fix:** Check `pipe.get_sync_time()`, then either force a wider pull, increase `backtrack_minutes`, or run [`verify`](pipes/syncing.md) to repair a historical range chunk-by-chunk:

```bash
mrsm sync pipes -c plugin:foo -m bar --begin 2020-01-01
mrsm verify pipes -c plugin:foo -m bar --begin 2024-01-01 --end 2025-01-01
```

```python
pipe.sync(begin=None)  # no lower bound
pipe.update_parameters({'fetch': {'backtrack_minutes': 1440}})  # 24h backtrack
pipe.edit()
```

See [Syncing](pipes/syncing.md) for the full fetch → enforce → filter → write flow.

---

### Duplicate rows appear

**Symptom:** The same logical record appears multiple times in the target table.

**Cause:** Without index columns (`datetime`, `id`, and/or `primary`), Meerschaum cannot identify matching records and appends every sync. To update existing rows in place, `upsert` must be enabled.

**Fix:** Set your index columns and enable `upsert` if updates should overwrite existing rows:

```python
pipe.columns = {'datetime': 'timestamp', 'id': 'station_id'}
pipe.upsert = True  # creates a unique index and uses UPSERT
pipe.edit()
```

---

### `Detected minimum datetime greater than maximum datetime.`

**Symptom:** Warning logged during a sync.

**Cause:** Fetched timestamps are inverted—usually a swapped `begin`/`end`, a bad source query, or mixed timezone parsing.

**Fix:** Inspect `pipe.fetch(begin=..., end=...)` for malformed timestamps and confirm `begin <= end`.

---

## Datetimes & timezones

### Timezone-aware vs naive mismatch

**Symptom:** Incremental syncs miss matches, comparisons behave unexpectedly, or datetime cast warnings appear.

**Cause:** By default, `datetime` columns are stored timezone-aware (`TIMESTAMPTZ` / UTC). Mixing naive and tz-aware values breaks window comparisons.

**Fix:** Check `pipe.tzinfo` (`UTC`, `None`, etc.). Normalize incoming timestamps to UTC, or explicitly register a naive datetime dtype under `pipe.dtypes` (see [Parameters](pipes/parameters.md)).

---

### `NaT` / nulls in the datetime axis

**Symptom:** Sync fails or drops rows when the datetime column contains nulls.

**Cause:** Incremental windows require a defined position on the `datetime` axis, so null/`NaT` values are invalid there.

**Fix:** Filter or backfill nulls in the source, or move the nullable column out of the datetime index. For nulls in *other* index columns, set `null_indices: True` (see [Parameters](pipes/parameters.md)).

---

## Dtypes & schema

### `Detected different types for '<col>' (... vs ...), falling back to 'object'...`

**Symptom:** Warning during sync; the column widens to generic text/object.

**Cause:** Incoming data changed type between syncs (e.g. integers one run, strings the next), so Meerschaum widens the column to avoid data loss.

**Fix:** Fix the source type and pin the column under `pipe.dtypes` (see [Parameters](pipes/parameters.md)):

```python
pipe.dtypes = {'reading': 'numeric'}  # or 'Int64', 'float64', etc.
pipe.edit()
```

---

### Column type changed or new column not added

**Symptom:** A column's type altered unexpectedly, or a new source field never appears in the table.

**Cause:** By default, Meerschaum auto-alters schemas (`static: False`, `enforce: True`). Setting `static: True` locks the schema (blocking new columns and alters), and `enforce: False` skips dtype enforcement and new-column detection.

**Fix:** Lock a schema with `pipe.parameters['static'] = True` (or pin individual `dtypes` before the first sync). To allow new columns again, set `static: False` and keep `enforce: True`.

```python
pipe.parameters['static'] = True
pipe.edit()
```

---

## Upserts & partitioned tables

### Upsert fails: `ON CONFLICT` has no matching unique constraint

**Symptom:** Database rejects an upsert with a missing unique/exclusion constraint error (PostgreSQL, MySQL, or MSSQL).

**Cause:** On partitioned tables (TimescaleDB hypertables or native range-partitioned tables—default for datetime pipes), the partition column (`datetime`) is part of the composite primary key. An `ON CONFLICT` target on the `primary` key alone cannot match it.

**Fix:** Meerschaum automatically includes `[datetime_col, primary_key]` when `columns['datetime']` is set. Confirm `columns['datetime']` is defined, or disable partitioning *before* creating the table (see [Maintenance](pipes/maintenance.md)):

```python
pipe.parameters['hypertable'] = False
pipe.edit()
```

---

## Connectors & instances

### `Unable to parse connector keys` / `Cannot create Connector`

**Symptom:** An action cannot resolve a connector or reports an unknown connector type.

**Cause:** Connector keys must follow `type:label` (e.g. `sql:main`, `api:mrsm`, `plugin:noaa`), the connector isn't defined in config/env, or a custom `@make_connector` plugin isn't loaded.

**Fix:**

- Check defined connectors with `mrsm show connectors` and verify `-i` / `mrsm_instance`.
- Add a connector with `mrsm bootstrap connector` or edit `MRSM_ROOT_DIR/config/connectors.yaml`.
- Or define one on the fly via an environment URI (see [Environment](environment.md)):

```bash
export MRSM_SQL_MAIN='postgresql://user:pass@host:5432/dbname'
export MRSM_API_MRSM='https://user:pass@api.example.com'
```

---

## API instances & auth

### Permission or authentication errors (`401` / `403`)

**Symptom:** Requests to an `api:` instance fail with `401`, `403`, or login errors.

**Cause:** Missing or expired credentials, or the user/token lacks the required role or scope.

**Fix:** Run `mrsm login` (or `mrsm bootstrap connector` to store credentials), pass credentials in `MRSM_API_<LABEL>`, or use a scoped API token for automation. Confirm the account's role on the server; see [API Instance](api-instance/index.md) and [Permissions](api-instance/permissions.md).

---

## Plugins

### Plugin dependencies fail / `ImportError`

**Symptom:** A plugin fails to import or cannot find packages listed in `required`.

**Cause:** Each plugin uses its own virtual environment; `setup` hasn't run or new dependencies were added after install.

**Fix:** Reinstall plugin dependencies and import heavy libraries lazily with `attempt_import(..., venv='<plugin_name>')`:

```bash
mrsm install plugin <name>   # or: mrsm setup plugins <name>
```

See [Plugins](plugins/index.md).

---

### Plugin changes not picked up

**Symptom:** Edits to a plugin file don't take effect in the running shell or server.

**Cause:** Plugins and their manifest are cached in memory.

**Fix:** Rebuild the manifest and recycle workers:

```bash
mrsm reload plugins
```

---

### Stale `~/.local` install shadows your working tree

**Symptom:** Tests or `python -m meerschaum` run an old version despite an up-to-date checkout.

**Cause:** An older `meerschaum` under `~/.local/lib/pythonX.Y/site-packages/` sits ahead of your repo on `sys.path`. (Python workers and the API host refuse to import a mismatched major or v4 package.)

**Fix:** Run `mrsm doctor` to locate the stale copy and remove/upgrade it, or put your repo first on `PYTHONPATH`:

```bash
PYTHONPATH=$(pwd) python -m pytest ...
python -c "import meerschaum; print(meerschaum.__file__, meerschaum.__version__)"
```

---

## Debugging tips

| Tool | What it shows |
|---|---|
| `mrsm ... --debug` | Resolved SQL, sync windows, dtypes, engine plans, and worker calls. |
| `mrsm show pipes -i <inst>` | Registered pipes matching your filters on the target instance. |
| `mrsm show columns` / `show rowcounts` | Stored table schema and row counts. |
| `pipe.get_sync_time()` | Current upper bound of the datetime axis. |
| `pipe.parameters` | Full pipe config (`columns`, `dtypes`, `fetch`, `upsert`, `static`, `hypertable`). |
| `pipe.get_data(begin=..., end=..., params=...)` | Sample of rows actually stored in the table. |

### Debug output

Pass `--debug` to any command, or toggle `debug on` / `debug off` in the interactive shell (`mrsm --debug` starts the shell with debug on). Each line prints a UTC timestamp and the originating Rust or Python module:

```text
 🐞 14:03:07.512 | meerschaum_cli::dispatch:935
Action ["sync", "pipes"] -> `sync pipes` (_sync_pipes), sub-actions [].
 🐞 14:03:07.519 | meerschaum_connector_sql::driver:707
SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = 'mrsm_pipes'
```

What `--debug` logs:

- Resolved action and parsed arguments (passwords, tokens, secrets, and URI credentials redacted as `***`).
- Opened `sql:`, `api:`, and `valkey:` connections (flavor, host, port, database, username; never passwords).
- Every native SQL statement and bound parameter count.
- Sync stages with row counts and timings (fetched chunks, dtype enforcement, existing-row filter, writes).
- Syncing Engine plans, node transitions, and chunk bounds/timings.
- Python worker calls (forwarding `debug=True` so plugin `dprint()` messages appear too) and plugin discovery.

Debug output always writes to **stderr** (including Python's `meerschaum.utils.debug.dprint()` in v5), keeping stdout clean for redirection while still appearing in job logs. Customize styling via `formatting:debug`, or disable colors/icons with `NO_COLOR`, `formatting:ansi: false`, or `formatting:unicode: false` (see [Colors and Plain Output](/reference/actions/#colors-and-plain-output)). During full-screen views (`watch run`, `watch jobs`, shell F2/F3), up to 2,000 messages buffer until you exit the view.

From Python, pass `debug=True` to pipe/connector methods (`pipe.sync(debug=True)`, `pipe.get_data(debug=True)`) or wrap any block with `debugging()`:

```python
import meerschaum as mrsm
from meerschaum._core import debugging

with debugging():
    print(mrsm.Pipe('sql:main', 'demo').exists())
```

Debug mode is process-wide while active (except on the API server, where a client's `debug=True` stays scoped to that action). `RUST_LOG` is separate and controls Rust `tracing` output for developers.

---

## See also

- [Glossary](glossary.md)
- [Syncing](pipes/syncing.md)
- [Parameters](pipes/parameters.md)
- [Maintenance](pipes/maintenance.md)
- [Environment](environment.md)
