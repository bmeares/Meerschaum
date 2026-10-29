# Troubleshooting

A symptom-driven guide to the most common Meerschaum issues. Each entry follows **Symptom → Cause → Fix**.

!!! tip "First step: re-run with `--debug`"
    Almost every issue is faster to diagnose with the `--debug` flag. It prints the actual SQL, the resolved sync window, and the dtypes being applied. See [Debugging tips](#debugging-tips) at the bottom of this page.

For terminology used below, see the [glossary](glossary.md). For everything else, visit [docs.meerschaum.io](https://docs.meerschaum.io).

---

## Start here: `mrsm doctor`

!!! tip "Run `mrsm doctor` first"
    One command checks the installation and tells you what is wrong and how to fix it. It is **read-only** and takes about a second: it never installs, creates, migrates or writes anything, not even on a new root (no default config files, no plugin manifest, no entry-point cache), and it never imports plugin code. It reads the plugin manifest the last command cached and warns when it is stale or missing (`mrsm reload` rebuilds it). The one caveat: opening an existing SQLite database may create its `-wal` and `-shm` files beside it. Paste its `--nopretty` output into a bug report.

```bash
mrsm doctor                  # a table: group, check, PASS / WARN / FAIL / SKIP, and the fix for each problem
mrsm doctor -i sql:main      # check another instance
mrsm doctor --nopretty       # one JSON document, for a bug report or a script
mrsm doctor --debug          # the time each check took
```

In the SDK, importing `meerschaum.actions` initializes the action registry and discovers plugins before any action runs. A subsequent `actions['doctor'](...)` call is read-only from that point; `mrsm.entry(['doctor', ...])` also avoids that initial registry import.

Every check is **PASS**, **WARN** (works, but look at it), **FAIL** (something is broken) or **SKIP** (not applicable here, the detail says why); each WARN or FAIL carries the exact command or setting that fixes it. The command fails (and exits `1`) only when a check FAILs. `--nopretty` exits `1` on a FAIL too (v4 always exited `0` with `--nopretty`), and a script can also read `summary.ok` (and `summary.fail`) of the JSON document. A sample on a machine with a Python plugin installed, where an old `meerschaum` 4.x in the user site shadows the one `mrsm` was built with:

```text
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Group      ┃ Check              ┃ Status ┃ Detail                                                ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ install    │ meerschaum         │ PASS   │ 5.0.0; Python extension, in-process                   │
│            │ worker python      │ PASS   │ Python 3.13.1 at /usr/bin/python3                     │
│            │ meerschaum package │ FAIL   │ /usr/bin/python3 imports `meerschaum` 4.1.4 from      │
│            │                    │        │ /home/user/.local/lib/python3.13/site-packages/       │
│            │                    │        │ meerschaum (the user site), but this program is       │
│            │                    │        │ 5.0.0                                                 │
│            │                    │        │ Fix: remove or upgrade the stale copy                 │
│            │                    │        │ (`/usr/bin/python3 -m pip install --upgrade           │
│            │                    │        │ meerschaum`, or delete that directory), or set        │
│            │                    │        │ MRSM_PYTHON to the interpreter that has 5.0.0         │
│ paths      │ root               │ PASS   │ /home/user/.config/meerschaum (writable)              │
│            │ disk: root         │ PASS   │ /home/user/.config/meerschaum: 11.3 GB free of        │
│            │                    │        │ 214.9 GB, inodes 90% free                             │
│            │ disk: temp         │ WARN   │ /tmp: 2.1 GB free of 8.0 GB, inodes 3% free           │
│            │                    │        │ Fix: free space on the filesystem of /tmp (or point   │
│            │                    │        │ it elsewhere: MRSM_ROOT_DIR, TMPDIR)                  │
│ config     │ files              │ PASS   │ 4 config files parsed                                 │
│ instance   │ connection         │ PASS   │ sql:main (timescaledb) at localhost:5432/meerschaum   │
│            │ server version     │ PASS   │ PostgreSQL 18.1                                       │
│            │ tables             │ PASS   │ mrsm_pipes, mrsm_users, mrsm_plugins exist            │
│ extras     │ api                │ PASS   │ fastapi 0.141.1, uvicorn 0.38.0, jwt 2.13.0           │
│ ...        │                    │        │                                                       │
└────────────┴────────────────────┴────────┴───────────────────────────────────────────────────────┘

 💢 1 check failed (install: meerschaum package); 20 passed, 1 warning, 1 failed, 7 skipped.
```

What each group looks at:

| Group | Checks |
|---|---|
| `install` | The version and how this process runs (Python extension in-process, or a standalone binary), the build, the platform, the worker interpreter (`MRSM_PYTHON`, else `python3`) and its version, and the `meerschaum` package that interpreter imports: the same rule the worker and API host apply before importing it: a copy of another **major** version (a stale v4 in `~/.local`), a copy that looks like v4, or one speaking another worker protocol is a FAIL that names the path when Python plugins are installed (they run in that interpreter) and a WARN otherwise; another release of the same major version is a WARN naming both versions (the worker starts and warns once); extra copies on `sys.path` are a WARN. Plugins, Python connectors and the API host run in that interpreter. |
| `paths` | The root, config and plugins directories exist and are writable; free space and free inodes of the root's filesystem and of the temporary directory (WARN under 1 GB or 5% inodes, FAIL under 64 MB or 1% inodes: a full `/tmp` breaks workers). |
| `config` | The config files parse, no unknown top-level keys or removed v4 keys (`system:experimental:join_fetch: true`), the `MRSM_<TYPE>_<LABEL>` connector variables found (names only, never values; other `MRSM_*` variables are listed apart), the default instance. |
| `instance` | A connection to the default instance (or `-i`) with a 5 second limit, the server version and the Meerschaum tables (`mrsm_pipes`, `mrsm_users`, `mrsm_plugins`) for `sql:` instances; `/healthcheck` and `/version` of an `api:` instance (WARN if its version differs from the client's). A SQLite database that does not exist yet is reported, not created. With the public Python DuckDB transport, the live query is SKIPped to keep diagnostics read-only: no database or shared anchor is opened. Instances are read from the config, so a connector registered only through Python (`mrsm.get_connector(...)` with no config entry) is not known to `doctor`. |
| `drivers` | In a Python host using the public DuckDB transport, installed driver metadata and the existing policy are checked without importing DuckDB or selecting a policy. A metadata PASS does not verify importability or connectivity; missing version metadata is a WARN. Standalone/native DuckDB and Oracle use the shared-library checks (`mrsm show drivers`): a missing library is a WARN when a configured connector needs it, a FAIL for the default instance when nothing will fetch it (Oracle always; native DuckDB with `MRSM_NO_AUTO_INSTALL` or a load error), and a WARN for native DuckDB downloaded on first use. A missing native library is never installed by the doctor. |
| `extras` | Which extras (`pandas`, `polars`, `geo`, `sql`, `api`, `dash`, `shell`) the worker interpreter can import, and a WARN when an installed plugin needs one that is missing (API-host plugins need `api`, Dash plugins `dash`). An extra that is partly installed and that no plugin needs is a PASS naming the packages that are not installed. |
| `plugins` | Whether the cached plugin manifest is current (a missing or stale one is a WARN: `mrsm reload`), how many plugins are installed, the ones whose import failed, and virtual environments built by another Python than the worker's (or whose interpreter is gone). Plugin code is never imported by the doctor. |
| `jobs` | Jobs that are running, paused or stopped, jobs whose last run failed, and stale PID files. |
| `stack` | If the Docker stack's compose file exists and Docker answers, the state and health of each service (a service stopped on purpose is a WARN, a crashed or unhealthy one a FAIL). The stack is never touched. |
| `extensions` | The PostgreSQL extensions of the instance against what the server ships (see [Upgrading the stack](/reference/stack/upgrades/)). |

---

## Syncing

### Sync succeeds but no rows land

**Symptom:** `sync pipes` reports success (often `Inserted 0 rows, updated 0 rows`) but the target table stays empty or unchanged.

**Cause:** This is almost always one of:

1. **Deduplication.** `pipe.filter_existing(df)` diffs incoming rows against the overlapping window already stored. Identical rows are dropped, nothing new to insert.
2. **The sync window excluded your data.** With `begin=''` (the default), the lower bound is the pipe's *sync time* minus `fetch:backtrack_minutes`. Rows older than that window are never fetched.
3. **Backtrack too small for late-arriving / corrected data.** If a source updates rows slightly behind the latest timestamp, a small `backtrack_minutes` won't re-fetch them.

**Fix:**

- Confirm there is genuinely new data: `pipe.get_sync_time()` shows the current upper bound.
- To force a full historical re-pull, sync with an explicit lower bound or no bound:

    ```bash
    mrsm sync pipes -c plugin:foo -m bar --begin 2020-01-01
    ```

    ```python
    pipe.sync(begin=None)   # no lower bound at all
    ```

- Widen the backtrack window so corrections are caught:

    ```python
    pipe.update_parameters({'fetch': {'backtrack_minutes': 1440}})  # re-fetch last 24h
    pipe.edit()
    ```

- To re-check and repair a historical range chunk-by-chunk, use [`verify`](pipes/syncing.md) instead of `sync`:

    ```bash
    mrsm verify pipes -c plugin:foo -m bar --begin 2024-01-01 --end 2025-01-01
    ```

See [Syncing](pipes/syncing.md) for the full fetch → enforce → filter → write flow.

---

### Duplicate rows appear

**Symptom:** The same logical record shows up multiple times in the target table.

**Cause:** Without a uniqueness definition, Meerschaum has no way to know two rows are "the same", so every sync appends. Uniqueness comes from the `datetime` and `id` index columns (and/or a `primary` column). If those aren't set, or if you want existing rows updated in place rather than appended, you need `upsert`.

**Fix:**

- Define index columns so rows can be deduplicated:

    ```python
    pipe.columns = {'datetime': 'timestamp', 'id': 'station_id'}
    pipe.edit()
    ```

- To update matching rows in place instead of inserting new ones, enable upsert:

    ```python
    pipe.upsert = True
    pipe.edit()
    ```

    This creates a unique index on the index columns and switches the write to an UPSERT.

---

### `Detected minimum datetime greater than maximum datetime.`

**Symptom:** This warning appears during a sync.

**Cause:** The incoming data's earliest timestamp is later than its latest, usually a malformed source query, a swapped `begin`/`end`, or mixed timezone parsing producing nonsensical ordering.

**Fix:** Inspect the fetched frame (`pipe.fetch(begin=..., end=...)`) and check the `datetime` column for parsing errors. Make sure `begin <= end` if you passed them explicitly.

---

## Datetimes & timezones

### Timezone-aware vs naive mismatch

**Symptom:** Rows silently fail to match on incremental syncs, comparisons behave oddly, or you see datetime cast warnings.

**Cause:** The pipe's `datetime` column is stored timezone-aware (`TIMESTAMPTZ` / UTC) by default, but the incoming data is naive (or vice versa). Mixing the two breaks window comparisons.

**Fix:**

- Check the column's timezone with `pipe.tzinfo` (returns `UTC`, `None`, etc.).
- Make incoming timestamps tz-aware (UTC is the convention), or explicitly opt into naive storage by registering the column dtype without a timezone. See the dtype list in [Parameters](pipes/parameters.md).

---

### `NaT` / nulls in the datetime axis

**Symptom:** Sync errors or rows dropped; the datetime axis has missing values.

**Cause:** The `datetime` index column drives the incremental sync window, null (`NaT`) values there have no defined position.

**Fix:** Clean the source so the datetime column is always populated, or move the nullable field out of the index. If you need nulls allowed in *other* index columns, set `null_indices: True` (see [Parameters](pipes/parameters.md)).

---

## Dtypes & schema

### `Detected different types for '<col>' (... vs ...), falling back to 'object'...`

**Symptom:** This warning during a sync, followed by the column being stored as a generic object/text type.

**Cause:** Incoming data for a column has a different type than what's already stored (e.g. ints one sync, strings the next). Meerschaum widens the column to `object` to avoid data loss.

**Fix:**

- Pin the column's dtype explicitly so incoming data is coerced consistently:

    ```python
    pipe.dtypes = {'reading': 'numeric'}   # or 'Int64', 'float64', etc.
    pipe.edit()
    ```

- Fix the source so the type is stable across syncs.

See the supported dtype strings in [Parameters](pipes/parameters.md).

---

### Column type changed after the first sync

**Symptom:** A column's database type isn't what you expected, or it changed on a later sync.

**Cause:** By default, Meerschaum auto-alters the schema, it adds new columns and widens types as incoming data evolves.

**Fix:**

- To **lock the schema** (no new columns, no alters), set `static: True`:

    ```python
    pipe.parameters['static'] = True
    pipe.edit()
    ```

- To pin individual column types, register them under `dtypes` (above) before the first sync.

---

### New column not added

**Symptom:** A field present in the incoming data never appears in the target table.

**Cause:** One of:

- `static: True`, schema changes are disabled entirely.
- `enforce: False`, dtype enforcement (which also captures new columns) is skipped, so untracked columns may be ignored.

**Fix:** Remove `static`/set it to `False` to allow auto-alter, and leave `enforce` at its default (`True`) unless you have a specific reason to skip enforcement. See [Parameters](pipes/parameters.md).

---

## Upserts & partitioned tables

### Upsert fails: `ON CONFLICT` has no matching unique constraint

**Symptom:** An upsert sync raises a database error complaining there is no unique/exclusion constraint matching the `ON CONFLICT` specification (PostgreSQL), or an equivalent error on MySQL/MSSQL.

**Cause:** On a **partitioned table** (a TimescaleDB hypertable *or* a native range-partitioned PostgreSQL / MySQL / MSSQL table, the default for datetime-axis pipes since v3.4.0), the partition column (your `datetime` column) is folded into the composite primary key. An upsert conflict target of the `primary` key alone therefore has no matching unique constraint, and the database rejects it. The conflict target **must include the datetime column** on partitioned tables.

**Fix:** Meerschaum handles this automatically, the conflict/join columns for a partitioned upsert are `[datetime_col, primary_key]`. If you hit this with a custom setup:

- Ensure your `columns['datetime']` is set so the partition column is known.
- Or opt the pipe out of native partitioning by setting `hypertable: False` before the table is created (only affects *newly* created tables, pre-existing plain tables aren't retroactively partitioned).

    ```python
    pipe.parameters['hypertable'] = False
    pipe.edit()
    ```

See the native-partitioning notes in [Maintenance](pipes/maintenance.md) and [Parameters](pipes/parameters.md).

---

## Connectors & instances

### `Unable to parse connector keys '<keys>'` / `Invalid connector keys`

**Symptom:** An action fails to resolve a connector.

**Cause:** Connector keys must be of the form `type:label` (e.g. `sql:main`, `api:mrsm`, `plugin:noaa`). A bare label, wrong type, or typo won't resolve.

**Fix:**

- List configured connectors: `mrsm show connectors`.
- Check the instance keys you're passing (`-i` / `--instance` / `mrsm_instance`) point at a real, configured instance.
- Add or edit a connector with `mrsm bootstrap connector` or by editing `MRSM_ROOT_DIR/config/connectors.yaml`.

---

### `Cannot create Connector of type '<type>'.`

**Symptom:** This warning when creating a connector.

**Cause:** The connector type isn't registered (no matching built-in or custom `@make_connector` class), or its config block is missing required attributes.

**Fix:** Verify the type is spelled correctly and that any custom connector plugin is installed/loaded. For SQL/API connectors you can supply config inline via a URI environment variable (next entry).

---

### Setting a connector via environment variable (URI)

**Symptom:** You want to point a connector at a database/API without editing config files.

**Cause:** N/A, this is the intended override mechanism.

**Fix:** Set `MRSM_SQL_<LABEL>` or `MRSM_API_<LABEL>` to a URI. The label becomes the connector's label (lowercased):

```bash
export MRSM_SQL_MAIN='postgresql://user:pass@host:5432/dbname'
export MRSM_API_MRSM='https://user:pass@api.example.com'
```

`MRSM_SQL_MAIN` above defines the `sql:main` connector. See [Environment](environment.md).

---

## API instances & auth

### Permission / authentication errors against an API instance

**Symptom:** Requests to an `api:` instance fail with `401`/`403`, "permission denied", or login failures.

**Cause:** The API connector has no valid credentials, the token/session expired, or the account lacks permission for the operation (e.g. registering pipes/plugins on a locked-down server).

**Fix:**

- Log in interactively: `mrsm login`, or bootstrap the connector with `mrsm bootstrap connector` to store credentials.
- Embed credentials in the URI (`MRSM_API_<LABEL>=https://user:pass@host`), see above.
- For non-interactive jobs, use an API token. See [API instance](api-instance/index.md).
- Confirm the server is reachable and the account has the needed role.

---

## Plugins

### Plugin dependencies fail to install / `ImportError` in a plugin

**Symptom:** A plugin errors on import, or its `required` packages aren't found at runtime.

**Cause:** Each plugin installs its dependencies into a **plugin-named virtual environment**. If `setup` hasn't run, or a dependency was added after install, the venv is stale.

**Fix:**

- Reinstall the plugin's dependencies:

    ```bash
    mrsm install plugin <name>   # or: mrsm setup plugins <name>
    ```

- Inside plugin/connector code, import heavy packages with `attempt_import(..., venv='<plugin_name>')` rather than a top-level import.

See [Plugins](plugins/index.md).

---

### Plugin changes not picked up

**Symptom:** You edited a plugin but Meerschaum runs the old code.

**Cause:** Plugins are cached in the running process / shell session.

**Fix:** Reload plugins (or restart the shell):

```bash
mrsm reload plugins
```

---

### Stale `~/.local` install shadows your working tree (developers)

**Symptom:** Running tests or `python -m meerschaum` reflects an *old* version (new modules are missing, behavior is outdated) even though your repo is up to date.

**Cause:** A separately installed `meerschaum` under `~/.local/lib/pythonX.Y/site-packages/` is earlier on `sys.path` than the repo checkout and shadows it.

**Fix:** `mrsm doctor` names the stale copy (`install: meerschaum package` reports its path). A Python worker (and the API host) also refuses to import a `meerschaum` of another major version, one that looks like v4, or one speaking another worker protocol (importing v4 would run its start-up, which installs packages whatever `MRSM_NO_AUTO_INSTALL` says), and says so once; for another release of the same major version it starts and warns once. Force the repo onto the path:

```bash
PYTHONPATH=$(pwd) python -m pytest ...
```

Verify with `import meerschaum; print(meerschaum.__file__, meerschaum.__version__)`. (The project's `scripts/test.sh` sets this up correctly.)

---

## Debugging tips

- **`--debug`**, add to any action to print resolved SQL, sync windows, and dtype enforcement. The single most useful diagnostic. See [Debug output](#debug-output) below for what it prints and where.

    ```bash
    mrsm sync pipes -c plugin:foo -m bar --debug
    ```

- **`mrsm show pipes`**, list the pipes matching your key filters; confirm the pipe exists and resolves to the instance you expect. Add `-i <instance>` to target a specific instance.

- **`pipe.get_sync_time()`**, the current upper bound of the datetime axis. If this is unexpectedly recent, your sync window is excluding older data.

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe('plugin:foo', 'bar')
    print(pipe.get_sync_time())
    ```

- **`pipe.parameters`**, inspect the full metadata dict: `columns`, `dtypes`, `fetch`, `upsert`, `static`, `enforce`, `hypertable`, etc. Most "why is it doing that?" questions are answered here.

    ```python
    import json
    print(json.dumps(pipe.parameters, indent=2, default=str))
    ```

- **`mrsm show columns` / `mrsm show rowcounts`**, confirm the stored schema and row counts match expectations.

- **`pipe.get_data(begin=..., end=..., params=...)`**, pull a sample directly to see what's actually stored.

### Debug output

`--debug` works on every action, and the shell's `debug on` toggle turns it on for every command
you type (and for the shell itself) until `debug off`. Each message starts with a header naming
the time (UTC) and the code that printed it, like v4's `dprint()`:

```text
 🐞 14:03:07.512 | meerschaum_cli::dispatch:935
Action ["sync", "pipes"] -> `sync pipes` (_sync_pipes), sub-actions [].
 🐞 14:03:07.519 | meerschaum_connector_sql::driver:707
SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = 'mrsm_pipes'
```

Rust code names its module (`meerschaum_connector_sql::driver`), Python code its module
(`meerschaum.connectors.sql._pipes`) or your plugin's. With `--debug` you see:

- the action the words resolved to and the parsed arguments. Secrets are printed as `***`:
  values of keys or flags named like `password`, `secret`, `token` or `api_key` (also inside
  `--params` and other JSON), URI passwords and secret query parameters (`?password=`);
- each connection opened (`sql:`, `api:` and `valkey:`: flavor, host, port, database, username;
  never the password);
- every SQL statement the native SQL connector runs (with the number of bound parameters);
- the sync stages with row counts and timings: fetch (each fetched chunk with its bounds, for
  chunked and generator sources too), dtype enforcement, the filter against existing rows
  (unseen vs. updated) and the write;
- the Syncing Engine's plan (action, reason, bounds and chunks per pipe) and each event as the
  run reaches it: nodes starting and finishing, every chunk with its bounds, row counts and
  duration;
- each call to a Python worker (operation, payload size, duration). `--debug` is forwarded to
  the worker, so `debug=True` reaches your plugin's `fetch()` / `sync()` / actions and their
  `dprint()` output appears too;
- plugin discovery and loading (native plugins, and Python plugins when the plugin manifest is
  rebuilt).

While a full-screen view (`watch run`, `watch jobs`, the shell's F2 / F3) is shown, debug output
is held back and printed when you leave the view (the last 2000 messages). `mrsm --debug` (or
`mrsm --shell --debug`) starts the shell with `debug` on; `debug off` turns it off.

Debug output goes to **stderr**, never stdout, so `mrsm ... --debug > out.txt` keeps the command's
real output clean, and job logs (which capture both streams) include it. In v5 Python's
`meerschaum.utils.debug.dprint()` also prints to stderr (v4 printed to stdout). The header is
colored with `formatting:debug` (`mrsm edit config`) when the output is styled (see
[Colors and Plain Output](/reference/actions/#colors-and-plain-output)); `formatting:ansi: false`
or `NO_COLOR` removes the colors and `formatting:unicode: false` replaces the icon with `DEBUG`.

From Python, `debug=True` on a pipe or connector method of a SQL instance (`pipe.sync(debug=True)`,
`pipe.get_data(debug=True)`, `pipe.clear(debug=True)`, `mrsm.get_pipes(debug=True)`, ...) shows
the Rust core's debug output for that call through `dprint()`. For any other call, wrap it in
`meerschaum._core.debugging()`:

```python
import meerschaum as mrsm
from meerschaum._core import debugging

with debugging():
    print(mrsm.Pipe('sql:main', 'demo').exists())
```

Debug output is on for the whole process while any `--debug` action, `debug=True` call or
`debugging()` block runs, so work running at the same time in other threads prints too. The API
server is the exception: an action a client runs with `debug` passes `debug=True` to the action
but does not turn on debug output for the server's other requests.

`RUST_LOG` is unrelated to `--debug`: it still prints the Rust `tracing` events its filter selects
(dependencies included) in the `tracing` format, for developers.

---

## See also

- [Glossary](glossary.md)
- [Syncing](pipes/syncing.md)
- [Parameters](pipes/parameters.md)
- [Maintenance](pipes/maintenance.md)
- [Environment](environment.md)
- [docs.meerschaum.io](https://docs.meerschaum.io)
