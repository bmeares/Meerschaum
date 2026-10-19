# meerschaum-connector-sql

> **Internal crate.** `meerschaum-connector-sql` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

The v5 SQL connector (see `docs/design/v5.0.0.md` §6.2 and D13): flavor-aware query building
ported from `meerschaum/utils/sql.py` and `meerschaum/connectors/sql/`, connection specs parsed
from the same `connectors.yaml` attributes and SQLAlchemy URIs as v4, and `sqlx` drivers that
return Arrow record batches.

## Drivers

| Family | Flavors | Driver | Status |
|---|---|---|---|
| PostgreSQL | `postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`, `cockroachdb` | `sqlx` | implemented |
| MySQL | `mysql`, `mariadb` | `sqlx` | implemented |
| SQLite | `sqlite`, `geopackage` | `sqlx` | implemented |
| MSSQL | `mssql` | `tiberius` (rustls) | implemented, cargo feature `mssql` (default) |
| Oracle | `oracle` | `oracle` (ODPI-C) | implemented, cargo feature `oracle`; Oracle Instant Client at runtime |
| DuckDB | `duckdb` | DuckDB's own `libduckdb` (v1.5.6), loaded at run time through its C API | implemented, cargo feature `duckdb` (default; adds only the loader) |


Python hosts register a lazy DuckDB transport factory at extension initialization. When the
public `duckdb >= 1.5.6` package is installed and `MRSM_DUCKDB_LIB` is unset, Rust instance
methods use that driver for database I/O, sharing an anchor with SQLAlchemy. Pipe algorithms,
query generation, pooling, and the instance layer stay Rust. Each duplicate connection has an
independent transaction; SDK memory identities are per connector object. Standalone and
base-only installs continue to use the C API described below. Explicit `MRSM_DUCKDB_LIB`
pins native mode; a native-selected Python process refuses SQLAlchemy DuckDB access before
optional imports to prevent incompatible snapshots and lost writes. Unset the override,
install `meerschaum[sql-duckdb]`, and restart to use both interfaces. Transport selection never
installs packages. Conflicting per-file settings and custom creator/module or unsupported
connection overrides are rejected before opening a database. Forked processes must start a
fresh interpreter instead of reusing opened DuckDB state.

Parameterized native reads accept one `SELECT` statement. The public driver materializes
bound relations and renames duplicate output columns, so the transport first binds a
metadata-only `DESCRIBE` to retain their original names, then evaluates the data plan once.
Use `SQLConnector.exec()` for writes or multiple statements; raw SQLAlchemy execution keeps
the driver's ordinary behavior.

A flavor whose feature is off is refused with `SqlError::DriverUnavailable`. The non-`sqlx`
drivers share the pool and operation layer in `driver/native.rs` (`NativeConn` per family).
`oracle` loads `libclntsh` when connecting (the build needs no Oracle libraries), so it is
opt-in. Nothing of DuckDB is compiled in: the `duckdb` feature (on by default) adds only a small
loader (`driver/duckdb/ffi.rs`, ~60 C API functions bound with `libloading`) and the downloader
(`duckdb_lib/`; `zip`, `fs4`, `reqwest`). The library is DuckDB's official release build,
looked up in this order: `MRSM_DUCKDB_LIB` (a file path), `<root>/drivers/duckdb/<version>/`, the
system loader path (on macOS and Windows only a file in an absolute system directory, never a bare name from the working directory; `show drivers` reports it without loading it), and finally a download of the release asset for the platform, checked against
a SHA-256 pinned in `duckdb_lib/install.rs` (blocked by `MRSM_NO_AUTO_INSTALL`, which turns it into
an error naming `mrsm install driver duckdb` and `MRSM_DUCKDB_LIB`). DuckDB v1.4 or newer works; the
installer fetches v1.5.6. Results are read from data chunks (`driver/duckdb/decode.rs`);
nested types, intervals, enums, bit strings, `TIME WITH TIME ZONE` and `BIGNUM` come back as the
text DuckDB itself prints. Every session is pinned to `TimeZone = 'UTC'` (the official build has
ICU, whose default is the host's zone), and aware timestamp parameters are bound as `TIMESTAMPTZ`
instants (see `PARITY.md`).

- **MSSQL**: statements run through `sp_executesql` with `@P1`, `@P2`, ... parameters; session
  `SET`s and batch-leading DDL (`CREATE SCHEMA`, `CREATE VIEW`, ...) run as plain batches.
  Parameters bound to `NULL` are inlined as a literal `NULL` (a typed TDS `NULL` cannot be
  converted to every column type). The bulk path is v4's `mssql_insert_json`
  (`INSERT ... SELECT ... FROM OPENJSON(@P1) WITH (...)`, one JSON document per chunk), used
  when `system:connectors:sql:bulk_insert:mssql` is on (the default); otherwise multi-row bound
  `INSERT`s. Native partitioning creates the partition function and scheme before
  `CREATE TABLE`, extends it with `SPLIT RANGE` before inserts, and drops both with the table.
- **GeoPackage** (`geopackage`): SQLite with GeoPackage binary geometry blobs (`GP` header,
  `srs_id` -1 as v4 writes it) and the `gpkg_*` metadata maintained by the connector
  (`instance/geopackage.rs`): GDAL's tables are created when missing, and every new pipe table's
  first geometry column is registered in `gpkg_contents` / `gpkg_geometry_columns` with its
  spatial reference. An index over that column (`indices: {geometry: <column>}`) is a
  GDAL-compatible R-tree (`rtree_<table>_<column>` plus GDAL's triggers); the connector registers
  the `ST_MinX` / `ST_MaxX` / `ST_MinY` / `ST_MaxY` / `ST_IsEmpty` functions those triggers call
  on its own connections (`driver/gpkg_functions.rs`). As with any GDAL-written GeoPackage, a
  plain SQLite client cannot insert into a spatially indexed table.

Arrow fields carry `meerschaum:db_type` (the driver's type name) and, for special columns,
`meerschaum:dtype` (`json`, `bytes`, `uuid`, `geometry`, `numeric`) and `meerschaum:encoding`
(`ewkb` for PostGIS, `mysql` for MySQL geometry). The keys are
`meerschaum_core::dtypes::field_meta`, shared with the kernel: `SqlDriver::read_frame` (or
`driver::batches_to_frame`) turns query results into a kernel `Frame` whose columns keep these
tags, so `enforce_dtypes` reads the declared dtypes instead of detecting them.

## Instance connector

`instance::SqlConnector` ports the SQL instance methods (`connectors/sql/_pipes.py`,
`_instance.py`, `tables/`, `_users.py`, `_plugins.py`, `_fetch.py`, `_partition.py`,
`_compress.py`, `_maintenance.py`) as async inherent methods with the v4 names, and implements
`meerschaum_connector::InstanceConnector` on top of them (`instance/trait_impl.rs`). The
`mrsm_pipes` / `mrsm_users` / `mrsm_plugins` / `mrsm_temp_tables` tables and the
`_mrsm_internal` schema keep v4's names and DDL, so a v4.1 instance is read as is.

- Query text comes from the builders in `queries/` (golden-tested); user values reach the
  database through v4's own escaping (`build_where`, `dateadd_str`) or as bound parameters.
- Rows are written with PostgreSQL `COPY ... CSV` (bulk insert, as v4) and multi-row bound
  `INSERT`s elsewhere, in one transaction, with v4's per-type serialization.
- `SqlConnector::sync` is the essentials of `Pipe.sync()` (dtype enforcement, autotime, capturing
  new special columns) over `sync_pipe`; `sync_pipe_inplace` syncs a `sql:` pipe whose source
  is the instance without reading rows into memory.
- Quirks kept on purpose are marked `// PARITY:`.

## Golden fixtures

`tests/fixtures/queries.json` (about 21k cases: every flavor and pipe shape, replayed byte for
byte by `tests/golden.rs`) and `tests/fixtures/uris.json` are generated from the Python implementation in this repository:

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-connector-sql/tests/fixtures/gen_queries.py
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-connector-sql/tests/fixtures/gen_uris.py
```

`tests/fixtures/instance_queries.json` (the instance methods' query text: `get_pipe_data_query`,
the `get_sync_time` / `get_pipe_rowcount` / `clear_pipe` queries, `get_add_columns_queries`,
`get_alter_columns_queries` and the instance tables' DDL, replayed by `tests/instance_golden.rs`)
is generated with every database read mocked, so no connection is needed:

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-connector-sql/tests/fixtures/gen_instance_queries.py
```

Use empty temporary directories for `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR` so no plugins or
real configuration are loaded; all fixture inputs are synthetic. The generators also run under
`scripts/parity/run.sh` and regenerate byte for byte.

## Cross-implementation test

`tests/instance_cross_impl.rs` syncs the pipes of `tests/fixtures/cross_impl/scenario.json` with
the Python implementation (`tests/fixtures/cross_impl/cross_impl.py`, run as a subprocess with
`PYTHONPATH` set to this repository) into one SQLite file and with Rust into another, then
compares `sqlite_master` DDL, `PRAGMA table_info`, row counts and stored values (with storage
classes), and has each implementation sync into and read the other's database. It uses
`MRSM_TEST_PYTHON` (default `.venv-ref/bin/python`, the released v4.1.4 reference; it runs with `-E`, so the interpreter itself must have `meerschaum` installed) and skips when the
interpreter is missing; `MRSM_TEST_KEEP=1` keeps the databases for inspection.

The MSSQL variant (`cross_implementation_mssql`, with `MRSM_TEST_MSSQL_URI`) syncs the same
pipes (except the in-place one) into four fresh `w4sql_xi_*` schemas and compares
`information_schema` dumps (column types, rows, indices) and both implementations' reads; it
drops the schemas afterwards.

`tests/instance_geopackage.rs` checks the GeoPackage metadata, binary geometries and R-tree
maintenance, and syncs the pipes of `tests/fixtures/geopackage/scenario.json` through v4
(`tests/fixtures/geopackage/gpkg_parity.py`) and through Rust into two GeoPackages. Both files
must read the same through GDAL (geopandas / pyogrio: layers, CRS, column dtypes, geometries)
and through each implementation's `get_data`. The Python half needs geopandas and pyogrio in
the reference interpreter (`MRSM_TEST_PYTHON`).

## Running the integration tests

The SQLite tests always run. The PostgreSQL-family and MySQL-family tests skip unless their URIs
are set. The driver tests create uniquely named `_mrsm_rs_test_*` tables; the instance tests
(`tests/instance_*.rs`, one test per flavor via `flavor_tests!`) prefix every pipe's connector
keys with a random `rs<id>` tag, register their pipes in the container's `mrsm_pipes` table and
delete them afterwards.

Start the databases from the repository's compose file:

```bash
docker compose -f tests/docker-compose.yaml up -d postgresql postgis mariadb mysql
```

Then run:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
MRSM_TEST_PG_URI=postgresql://test:test1234@localhost:5559/testdb \
MRSM_TEST_POSTGIS_URI=postgresql://test:test1234@localhost:5449/testdb \
MRSM_TEST_MYSQL_URI=mysql://test:test1234@localhost:3309/testdb \
    cargo test -p meerschaum-connector-sql --test driver_sqlite --test driver_postgres --test driver_mysql
```

`MRSM_TEST_MYSQL_URI=mysql://root:my-secret-pw@localhost:3310/mysql` targets the MySQL 5.7
service instead of MariaDB.

The instance tests read one URI per flavor:

```bash
export MRSM_TEST_PG_URI=postgresql://test:test1234@localhost:5559/testdb
export MRSM_TEST_TIMESCALE_URI=timescaledb://test:test1234@localhost:5439/testdb
export MRSM_TEST_TIMESCALE_HA_URI=timescaledb-ha://test:test1234@localhost:5459/testdb
export MRSM_TEST_POSTGIS_URI=postgis://test:test1234@localhost:5449/testdb
export MRSM_TEST_MYSQL_URI=mysql://root:my-secret-pw@localhost:3310/mysql
export MRSM_TEST_MARIADB_URI=mariadb://test:test1234@localhost:3309/testdb
cargo test -p meerschaum-connector-sql
``` Any PostgreSQL-family URI works for `MRSM_TEST_PG_URI`, including
the `timescaledb://` / `timescaledb-ha://` / `postgis://` schemes (e.g.
`timescaledb-ha://test:test1234@localhost:5459/testdb`).

`MRSM_TEST_TIMESCALE_HA_URI` separately enables the TimescaleDB-HA geometry SRID tests in
`tests/w17c_geometry_srid.rs`. Use the `timescaledb-ha://` scheme to exercise the PostGIS
column types; `timescaledb://` and `postgresql://` use text geometry storage, even on a server
with PostGIS installed. The tests compare insert and update SRIDs, inferred geometry types,
and the GeoPackage spatial reference with v4.1.4.

The other flavors read their own variables (unset skips; an empty value counts as set):

```bash
# MSSQL (`docker compose -f tests/docker-compose.yaml up -d mssql`).
export MRSM_TEST_MSSQL_URI='mssql://sa:supersecureSECRETPASSWORD123!@localhost:1439/master'
# GeoPackage and DuckDB: a directory for temporary database files (empty: the system temp dir).
export MRSM_TEST_GPKG_DIR=
export MRSM_TEST_DUCKDB_PATH=
# Oracle (needs Oracle Instant Client on the library path).
export MRSM_TEST_ORACLE_URI='oracle://system:oracle@localhost:1529/xe'
# DuckDB's shared library (not compiled in): `scripts/rust/duckdb_lib.sh` downloads it once into
# a persistent cache (checksum-verified) and prints the path; any DuckDB >= 1.4 library works.
export MRSM_DUCKDB_LIB="$(scripts/rust/duckdb_lib.sh)"
cargo test -p meerschaum-connector-sql                       # MSSQL, GeoPackage, DuckDB
cargo test -p meerschaum-connector-sql --features oracle     # + Oracle
```

Without `MRSM_DUCKDB_LIB` (and with no cached or system library) the DuckDB tests download the
library on first use unless `MRSM_NO_AUTO_INSTALL` is set, in which case they fail with the
message naming the fix. `tests/duckdb_library.rs` skips without `MRSM_DUCKDB_LIB`.

`tests/driver_native.rs` exercises the MSSQL / DuckDB / Oracle drivers directly. MSSQL's
instance schema must exist (v4 does not create it either); add `?schema=<name>` to the URI to
use one other than `dbo`.

## Engine state and cached metadata (wave 5)

`InstanceConnector` leases are implemented by an internal `mrsm_pipe_leases` table. A unique
SHA-256 pipe key arbitrates independent pools, conditional updates renew or take over expired
leases, and conditional deletes protect a replacement holder. Expiry uses the database clock,
not the caller's clock. Holder hashes preserve case-sensitive identity on case-insensitive
MySQL / MSSQL collations. `tests/instance_leases.rs` exercises two independent connections.

Small upserts (up to 64 rows) to `mrsm` pipes use one bound INSERT / MERGE statement.
MySQL / MariaDB and SQL Server store internal string keys as bounded VARCHAR / NVARCHAR
(255 characters) so their primary and composite keys can be indexed. Null or
repeated keys retain the general staging path (DuckDB internal writes reject these keys
explicitly, because v4 has no staged upsert template for DuckDB). Static temporary internal pipes cache their
metadata after creation; `drop_pipe` invalidates the cache. Warm engine state writes therefore
need one DML statement. `tests/w5sql.rs` checks this along with cached façade reads, SQLite UTC
sync times, raw document timestamps, and `{SQLITE_DB_PATH}` resolution.

Cold PostgreSQL-family internal pipe writes serialize table and index initialization with a
transaction advisory lock keyed by the physical schema and target in that database. A dedicated
connection keeps single-connection pools usable; closing it releases the lock on cancellation.
Connection and lock acquisition share the pool acquisition timeout and preserve the pool's TLS
and server runtime options. Cold callers check missing indices even when the table already exists,
skip existing ordinary indices, and never cache readiness after an index failure. Different targets
creating the same absent schema take a schema lock only during its DDL transaction; existing
schemas need no schema lock. Warm writes still use one DML statement, alongside existing native
partition maintenance where configured.
`tests/internal_initialization.rs` exercises independent one-connection pools and an existing table
missing its upsert key on PostgreSQL and PostGIS. CockroachDB skips advisory locks, which it does
not support; cross-process cold initialization there is still a gap.

`facade::sync_pipe_mut_cached` accepts the caller's cached column types, derives the pipe's
dtypes, and refreshes metadata after schema changes. The existing `sync_pipe_mut` remains
available. `get_pipe_data_and_state` shares the `DataQueryOptions`-aware read implementation.
`partition_pipe_with_parameters` returns both the success tuple and the patch it applied;
`partition_pipe` retains its original return type.

The sqlx drivers build native Arrow columns directly once output types are known. SQLite
retains first-batch affinity inference and falls back to cell coercion when later storage
classes differ. `read_docs_frame` preserves SQLite's raw timestamp strings (v4 `get_docs`),
and mixed SQLite storage classes remain heterogeneous object values. `read_frame` continues
to decode typed timestamps. Special decimal and geometry columns
retain their existing lossless conversion paths.

With the `duckdb` feature, runtime queries execute UTC timestamp arithmetic through
naive UTC timestamps before restoring TIMESTAMPTZ, avoiding the ICU-dependent interval
operator. The public v4 query renderer and its reference fixtures remain unchanged.

SQLite staged updates index the temporary table’s COALESCE key expressions to avoid repeated
full scans while preserving v4’s null-sentinel comparisons. `MRSM_CORE_TRACE=1` reports
filtering and SQL update stage times.

DuckDB pools share a single native database instance per canonical file path; connections
remain independent. This prevents separately opened instances from acquiring incompatible
leases against independent snapshots. DuckDB internal pipes use MERGE, since v4 skips
DuckDB indices. Lease tests also race two connections to take over an expired lease.

Oracle writes bind dates/timestamps natively and execute bulk rows in one transaction using
individual INSERT statements (pre-23c syntax, with correct identity sequence increments).
Oracle result labels follow SQLAlchemy's unquoted-name normalization.

### Wave 5 verification and remaining gaps

All runs used isolated roots/plugins (the Python suite used the documented `test_root` and
`tests/plugins`), disposable database ports, and `w5sql_` logical pipe/table prefixes. Harness
counts include unconfigured flavor cases which return early; each server URI was configured
in a separate run.

- `cargo fmt -p meerschaum-connector-sql` and `cargo fmt -p meerschaum-py` completed.
- Default `cargo clippy -p meerschaum-connector-sql --all-targets -- -D warnings` passed; the release
  `duckdb,oracle` feature combination also passed Clippy with all targets and denied warnings.
- Default SQL suite: 845 passed, including `instance_cross_impl` against released v4.1.4.
- Separate PostgreSQL, PostGIS, TimescaleDB, TimescaleDB-HA, MariaDB, MySQL, MSSQL, and GeoPackage
  full runs: 844 passed each before the final mixed-storage document regression was added.
  MSSQL also passed the server cross-implementation scenario. The strengthened
  `instance_leases` expiry-takeover race was rerun successfully on all those deployments.
- DuckDB full run with `--release --features duckdb,oracle`: 1,039 passed. The focused
  `instance_inplace`, `instance_leases`, and `w5sql` run passed 63 tests, including all three
  ICU-sensitive in-place cases, shared-file concurrency, and primary/composite internal MERGE.
- Oracle (`--features oracle`): the full run reported 935 passed and 7 failed. One failure was
  a test-table collision caused by identifier truncation discarding the random suffix; after
  shortening that test target, `test_autoincrement_primary_key::oracle` passed. Lease renewal,
  concurrent expiry takeover, internal primary/composite upserts, and inferred identities pass.
- Rust-mode SQLite pytest: 606 passed, 2 skipped, 3 deselected, with the documented exclusions.
- `scripts/parity/check_fixture_hygiene.sh crates/meerschaum-connector-sql/tests/fixtures`: clean.

Six Oracle cases remain unresolved (the full Oracle suite is **not** green):
`test_infer_numeric_from_mixed_types`, `test_sync_uuids_simple_upsert`,
`test_sync_change_columns_dtypes`, `test_sync_bools_inplace`, `test_fetch_pipes_keys`
(ORA-12704 character-set mismatch), and `test_sync_null_indices`. These need Oracle-specific
numeric/type evolution and general staged-update/key-query work; they are not being treated
as approved parity deviations. Oracle `analyze_pipe` deliberately returns unsupported, as
v4 does, and the test now asserts that behavior.

See `../meerschaum-py/PERF.md` for before/after 1M-row SQLite/PostGIS measurements. Large sync
and read steps beat Python in the final run, but millisecond-scale SQLite sync-time and
row-count calls remain slower: the every-step performance acceptance target is not yet met.

## Cross-pipe templates (wave 6)

`{{ Pipe(...) }}` references to *other* registered pipes resolve through
`meerschaum_connector::template` (`prepare_resolver` / `resolve_pipe`): the referenced pipes
(transitively, including `reference(s)`) are fetched asynchronously from their own instance, frozen
into a `SnapshotResolver`, and `Pipe::resolve_parameters_with` runs core's synchronous resolution on
that snapshot. A bare `{{ Pipe(...) }}` expands to `InstanceConnector::pipe_table_reference`
(`sql_item_name` with the instance schema on SQL instances, `pipe.target` elsewhere).
`SqlConnector::prepare_pipe` is the entry point; it runs in the `InstanceConnector` methods, `fetch`,
`sync`/`sync_pipe_result`/`sync_pipe_inplace` and the `facade` readers, and is a no-op for pipes
without `{{` or `reference(s)`. References naming another instance use
`SqlConnector::set_instance_provider`, defaulting to `MRSM_SQL_<LABEL>` URIs (`EnvSqlProvider`).
Raw parameters stay the persisted form (`register_pipe`, `edit_pipe`); resolved ones are for
execution. Limitation: `sync_pipe_mut` / `sync_pipe_mut_cached` (`&mut Pipe`) skip the resolver
because their in-memory parameter mutations would discard the snapshot.
`instance::plugins::version_ge` forwards to `meerschaum_core::version::version_ge`.

Internal (`mrsm`) string keys longer than 255 characters on MySQL / MariaDB / MSSQL fail with a
clear message instead of a driver error. A DuckDB internal upsert batch that repeats a key keeps
the last row; null keys are still rejected. `tests/w6b_templates.rs` covers all of this
(`w6b_` table prefix); the cross-implementation scenario gained a templated in-place child.
