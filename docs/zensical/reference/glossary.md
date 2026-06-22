# 📖 Glossary

This page defines the core Meerschaum domain terms in one place. Each entry links to its reference documentation where one exists. Other pages can deep-link to any term by its heading anchor, e.g. [`#upsert`](#upsert).

---

### Action

A top-level command such as `sync`, `show`, or `register`, implemented as a module in `meerschaum/actions/`. Every action returns a [`SuccessTuple`](#successtuple). Plugins add actions with the `@make_action` decorator.

### Subaction

A noun-specific variant of an [action](#action), auto-discovered by naming convention (e.g. `sync pipes` → `sync_pipes`). New subactions need no registration — just define `def <action>_<subaction>(**kwargs) -> SuccessTuple`.

### Backtrack (`backtrack_minutes`)

The overlapping window between syncs (default 1440 minutes), set under `parameters['fetch']['backtrack_minutes']`. During a [sync](#sync), rows are fetched starting from the last sync time *minus* this interval, so slower or backlogged data streams are not missed. See [Backtracking](/reference/pipes/syncing/#backtracking).

### Chunk (`chunk_minutes`)

The interval into which a [verification sync](#verification-sync) divides a pipe's range (default 43200 minutes — 30 days), set under `parameters['verify']['chunk_minutes']`. It is also the authoritative width for [native partitioning](#native-partitioning). Aliases: `chunk_hours`/`chunk_days`/`chunk_weeks`/`chunk_years`/`chunk_seconds`. See [Verification Syncs](/reference/pipes/syncing/#verification-syncs).

### Columns

The `parameters['columns']` dictionary maps a semantic *role* to a column name. The key roles are:

- **`datetime`** — the column driving incremental sync (the begin/end window); defines the [datetime axis](#datetime-axis).
- **`id`** — forms the composite uniqueness key alongside `datetime`.
- **`primary`** — the primary-key column.
- **`value`** — the measured value column.

Pipes without a `datetime` column still work but lose incremental behavior. See [Columns](/reference/pipes/parameters/).

### Connector

The interface Meerschaum uses to fetch and insert data and to read/write metadata. A connector is identified by [connector keys](#connector-keys) (`type:label`, e.g. `sql:main`). See [Connectors](/reference/connectors/).

### Connector keys

The two-part identifier of a [connector](#connector): its **type** and **label** joined by a colon, e.g. `sql:main` (type `sql`, label `main`) or `plugin:noaa`. As a pipe's first key, the connector keys name the data source.

### Datetime axis

The time dimension of a pipe, defined by the [`datetime` column](#columns). It may be a real timestamp (dtype `datetime`, stored as `TIMESTAMPTZ`) or an integer epoch (dtype `int`). Incremental syncs, chunking, and [native partitioning](#native-partitioning) all operate along this axis.

### Dtypes

The `parameters['dtypes']` dictionary mapping column names to Meerschaum dtype strings: `datetime`, `int`, `numeric`, `uuid`, `json`, `bytes`, `geometry[srid]`, or any valid Pandas dtype (e.g. `Int64`, `float64`, `bool`). Incoming data is cast to these during [sync](#sync). See [Parameters](/reference/pipes/parameters/).

### Executor

The backend that runs a [job](#job), set via `executor_keys`: `local` (a managed daemon process), `systemd` (a user service), or `api:label` (posted to a remote API instance). See [Jobs](/reference/background-jobs/).

### Hypercore / Columnstore

A TimescaleDB columnstore feature. With `hypercore=True` (default), the columnstore is enabled at `CREATE TABLE` and a columnstore policy is auto-created; `False` yields a plain row-store [hypertable](#hypertable). The columnstore policy *is* the compression policy (`compress`).

### Hypertable

Set by `parameters['hypertable']` (default `True`). On TimescaleDB, the target table is created as a hypertable. On other flavors supporting range partitioning (PostgreSQL/PostGIS, MySQL/MariaDB, MSSQL), datetime-axis pipes are range-partitioned by default — see [native partitioning](#native-partitioning). Set `False` to opt out. Only newly created tables are affected.

### In-place sync

A [sync](#sync) of a `sql:`-connector pipe whose data never leaves the database: the fetch, filter, and upsert all execute as SQL on the instance, avoiding a round-trip through the Meerschaum process.

### Instance connector

The [connector](#connector) where a pipe's metadata *and* data are stored — a pipe's 4th key (`instance=`/`mrsm_instance=`). Must implement the pipes/users/plugins/tokens interface (`IS_INSTANCE = True`). Implementations: `SQLConnector`, `APIConnector`, `ValkeyConnector`. See [Instances and Repositories](/reference/connectors/#instances-and-repositories).

### Job

A background process wrapping `sysargs`, run via `meerschaum.jobs.Job(name, sysargs, executor_keys=...)`. Jobs persist across restarts and stream logs; `start()`/`stop()`/`pause()` return a [`SuccessTuple`](#successtuple). See [Background Jobs](/reference/background-jobs/) and [executor](#executor).

### Location key

A pipe's optional third key (default `None`), used to tag or shard a data stream (e.g. a station or region). Two pipes sharing a [connector](#connector) and [metric](#metric-key) but differing in location are distinct pipes.

### Metric key

A pipe's second key — a label for the data stream it represents (e.g. `weather`, `sales`).

### `MRSM{}` config symlink

A reference to a configuration value inside pipe parameters or connector URIs, written `MRSM{key1:key2:key3}` and resolved at access time. For example, `MRSM{meerschaum:connectors:sql:main:password}` resolves to the configured password.

### Native partitioning

The default behavior (from `hypertable=True`) of range-partitioning a datetime-axis pipe's target table on its [datetime column](#columns) for flavors that support it (PostgreSQL/PostGIS, MySQL/MariaDB, MSSQL). Partition width reuses [`verify.chunk_minutes`](#chunk-chunk_minutes), with epoch-aligned (deterministic) boundaries. Partitions are auto-created during sync. See [Maintenance](/reference/pipes/maintenance/).

### Parameters

The central `pipe.parameters` metadata dictionary holding `columns`, `dtypes`, `indices`, `tags`, `fetch`, `verify`, `sql`, `target`, and behavioral flags (`upsert`, `hypertable`, `enforce`, etc.). Mutate in memory with `pipe.update_parameters({...}, persist=False)`; persist with `pipe.edit()` or `persist=True`. See [Parameters](/reference/pipes/parameters/).

### Pipe

A named data stream synced into a table, identified by three keys — [connector](#connector-keys), [metric](#metric-key), and [location](#location-key) — plus an [instance connector](#instance-connector) where its metadata and data live. The central abstraction in Meerschaum. See [Pipes](/reference/pipes/).

### Plugin

A Python file or package in `MRSM_PLUGINS_DIR` that extends Meerschaum. Recognized hooks include `register`, `fetch`, `sync`, and `setup`; decorators include `@make_action`, `@api_plugin`, `@dash_plugin`, and `@web_page`. See [Plugins](/reference/plugins/types-of-plugins/).

### Repository connector

A subset of [instance connectors](#instance-connector) used to host and distribute [plugins](#plugin). Repository connectors may only be `api` connectors. See [Instances and Repositories](/reference/connectors/#instances-and-repositories).

### Source connector

The [connector](#connector) named by a pipe's [connector keys](#connector-keys) — the origin of the pipe's data (e.g. `sql:main`, `plugin:noaa`). Distinct from the [instance connector](#instance-connector), which stores the result.

### `SuccessTuple`

The universal return type `Tuple[bool, str]` used by actions, pipe methods, and connector methods — e.g. `(True, "Success")` or `(False, "reason")`. Action-level code returns a `SuccessTuple` rather than raising.

### Sync

The main ETL entry point, `pipe.sync(df=None, begin='', end=None, ...)`, in three stages: **fetch** new rows, **filter** out already-seen rows, and **upsert** the remainder into the [instance connector](#instance-connector). See [Syncing](/reference/pipes/syncing/).

### Tags

A list of labels (`parameters['tags']`) for grouping pipes, usable as a selection filter (e.g. `show pipes --tags foo`). Prefix a tag with `_` to negate it. See [Tags](/reference/pipes/tags/).

### Target (table)

The table name a pipe writes to (`pipe.target`). Default: `{connector_keys with ':' → '_'}_{metric_key}` (plus `_{location_key}` when set). Override via `parameters['target']`. On SQL instances the full name is `schema.target`.

### Upsert

When `parameters['upsert']` is `True`, inserts and updates are combined into a single query and a unique index is created on the [columns](#columns). On partitioned tables the conflict target must include the [datetime column](#datetime-axis). See [`upsert`](/reference/pipes/parameters/#upsert).

### Verification sync

A more thorough [sync](#sync) (`verify pipes` / `pipe.verify()`) that divides a pipe's entire interval into [chunks](#chunk-chunk_minutes) and re-syncs each, comparing remote vs. local row counts to re-sync only mismatched chunks. See [Verification Syncs](/reference/pipes/syncing/#verification-syncs).

### `{{ Pipe() }}` syntax

A templating syntax for referencing another pipe from within pipe parameters (notably the `sql` definition), e.g. `{{ Pipe('plugin:noaa', 'weather') }}`, which resolves to the referenced pipe's [target table](#target-table) at query time. Supports attribute chains (`{{ Pipe('a', 'b').columns['datetime'] }}`) and self-reference (`{{ self.parameters['key'] }}`).
