# Troubleshooting

A symptom-driven guide to the most common Meerschaum issues. Each entry follows **Symptom → Cause → Fix**.

!!! tip "First step: re-run with `--debug`"
    Almost every issue is faster to diagnose with the `--debug` flag. It prints the actual SQL, the resolved sync window, and the dtypes being applied. See [Debugging tips](#debugging-tips) at the bottom of this page.

For terminology used below, see the [glossary](glossary.md). For everything else, visit [docs.meerschaum.io](https://docs.meerschaum.io).

---

## Syncing

### Sync succeeds but no rows land

**Symptom:** `sync pipes` reports success (often `Inserted 0 rows, updated 0 rows`) but the target table stays empty or unchanged.

**Cause:** This is almost always one of:

1. **Deduplication.** `pipe.filter_existing(df)` diffs incoming rows against the overlapping window already stored. Identical rows are dropped — nothing new to insert.
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

**Cause:** The incoming data's earliest timestamp is later than its latest — usually a malformed source query, a swapped `begin`/`end`, or mixed timezone parsing producing nonsensical ordering.

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

**Cause:** The `datetime` index column drives the incremental sync window — null (`NaT`) values there have no defined position.

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

**Cause:** By default, Meerschaum auto-alters the schema — it adds new columns and widens types as incoming data evolves.

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

- `static: True` — schema changes are disabled entirely.
- `enforce: False` — dtype enforcement (which also captures new columns) is skipped, so untracked columns may be ignored.

**Fix:** Remove `static`/set it to `False` to allow auto-alter, and leave `enforce` at its default (`True`) unless you have a specific reason to skip enforcement. See [Parameters](pipes/parameters.md).

---

## Upserts & partitioned tables

### Upsert fails: `ON CONFLICT` has no matching unique constraint

**Symptom:** An upsert sync raises a database error complaining there is no unique/exclusion constraint matching the `ON CONFLICT` specification (PostgreSQL), or an equivalent error on MySQL/MSSQL.

**Cause:** On a **partitioned table** (a TimescaleDB hypertable *or* a native range-partitioned PostgreSQL / MySQL / MSSQL table — the default for datetime-axis pipes since v3.4.0), the partition column (your `datetime` column) is folded into the composite primary key. An upsert conflict target of the `primary` key alone therefore has no matching unique constraint, and the database rejects it. The conflict target **must include the datetime column** on partitioned tables.

**Fix:** Meerschaum handles this automatically — the conflict/join columns for a partitioned upsert are `[datetime_col, primary_key]`. If you hit this with a custom setup:

- Ensure your `columns['datetime']` is set so the partition column is known.
- Or opt the pipe out of native partitioning by setting `hypertable: False` before the table is created (only affects *newly* created tables — pre-existing plain tables aren't retroactively partitioned).

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

**Cause:** N/A — this is the intended override mechanism.

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
- Embed credentials in the URI (`MRSM_API_<LABEL>=https://user:pass@host`) — see above.
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

**Symptom:** Running tests or `python -m meerschaum` reflects an *old* version — new modules are missing, behavior is outdated — even though your repo is up to date.

**Cause:** A separately installed `meerschaum` under `~/.local/lib/pythonX.Y/site-packages/` is earlier on `sys.path` than the repo checkout and shadows it.

**Fix:** Force the repo onto the path:

```bash
PYTHONPATH=$(pwd) python -m pytest ...
```

Verify with `import meerschaum; print(meerschaum.__file__, meerschaum.__version__)`. (The project's `scripts/test.sh` sets this up correctly.)

---

## Debugging tips

- **`--debug`** — add to any action to print resolved SQL, sync windows, and dtype enforcement. The single most useful diagnostic.

    ```bash
    mrsm sync pipes -c plugin:foo -m bar --debug
    ```

- **`mrsm show pipes`** — list the pipes matching your key filters; confirm the pipe exists and resolves to the instance you expect. Add `-i <instance>` to target a specific instance.

- **`pipe.get_sync_time()`** — the current upper bound of the datetime axis. If this is unexpectedly recent, your sync window is excluding older data.

    ```python
    import meerschaum as mrsm
    pipe = mrsm.Pipe('plugin:foo', 'bar')
    print(pipe.get_sync_time())
    ```

- **`pipe.parameters`** — inspect the full metadata dict: `columns`, `dtypes`, `fetch`, `upsert`, `static`, `enforce`, `hypertable`, etc. Most "why is it doing that?" questions are answered here.

    ```python
    import json
    print(json.dumps(pipe.parameters, indent=2, default=str))
    ```

- **`mrsm show columns` / `mrsm show rowcounts`** — confirm the stored schema and row counts match expectations.

- **`pipe.get_data(begin=..., end=..., params=...)`** — pull a sample directly to see what's actually stored.

---

## See also

- [Glossary](glossary.md)
- [Syncing](pipes/syncing.md)
- [Parameters](pipes/parameters.md)
- [Maintenance](pipes/maintenance.md)
- [Environment](environment.md)
- [docs.meerschaum.io](https://docs.meerschaum.io)
