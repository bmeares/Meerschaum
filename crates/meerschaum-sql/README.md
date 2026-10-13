# meerschaum-sql

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
| DuckDB | `duckdb` | `duckdb` 1.4 (bundled) | implemented, cargo feature `duckdb` |

A flavor whose feature is off is refused with `SqlError::DriverUnavailable`. The non-`sqlx`
drivers share the pool and operation layer in `driver/native.rs` (`NativeConn` per family).
`duckdb` compiles the bundled DuckDB C++ library (several minutes, about 250 MB of build
output) and `oracle` loads `libclntsh` when connecting (the build needs no Oracle libraries), so
both are opt-in. `duckdb` stays on 1.4, whose Arrow major matches the workspace's.

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
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_queries.py
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_uris.py
```

`tests/fixtures/instance_queries.json` (the instance methods' query text: `get_pipe_data_query`,
the `get_sync_time` / `get_pipe_rowcount` / `clear_pipe` queries, `get_add_columns_queries`,
`get_alter_columns_queries` and the instance tables' DDL, replayed by `tests/instance_golden.rs`)
is generated with every database read mocked, so no connection is needed:

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_instance_queries.py
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
`MRSM_TEST_PYTHON` (default `~/.config/meerschaum/venvs/mrsm/bin/python`) and skips when the
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
    cargo test -p meerschaum-sql --test driver_sqlite --test driver_postgres --test driver_mysql
```

`MRSM_TEST_MYSQL_URI=mysql://root:my-secret-pw@localhost:3310/mysql` targets the MySQL 5.7
service instead of MariaDB.

The instance tests read one URI per flavor:

```bash
export MRSM_TEST_PG_URI=postgresql://test:test1234@localhost:5559/testdb
export MRSM_TEST_TIMESCALE_URI=timescaledb://test:test1234@localhost:5439/testdb
export MRSM_TEST_POSTGIS_URI=postgis://test:test1234@localhost:5449/testdb
export MRSM_TEST_MYSQL_URI=mysql://root:my-secret-pw@localhost:3310/mysql
export MRSM_TEST_MARIADB_URI=mariadb://test:test1234@localhost:3309/testdb
cargo test -p meerschaum-sql
``` Any PostgreSQL-family URI works for `MRSM_TEST_PG_URI`, including
the `timescaledb://` / `timescaledb-ha://` / `postgis://` schemes (e.g.
`timescaledb-ha://test:test1234@localhost:5459/testdb`).

The other flavors read their own variables (unset skips; an empty value counts as set):

```bash
# MSSQL (`docker compose -f tests/docker-compose.yaml up -d mssql`).
export MRSM_TEST_MSSQL_URI='mssql://sa:supersecureSECRETPASSWORD123!@localhost:1439/master'
# GeoPackage and DuckDB: a directory for temporary database files (empty: the system temp dir).
export MRSM_TEST_GPKG_DIR=
export MRSM_TEST_DUCKDB_PATH=
# Oracle (needs Oracle Instant Client on the library path).
export MRSM_TEST_ORACLE_URI='oracle://system:oracle@localhost:1529/xe'
cargo test -p meerschaum-sql                       # MSSQL and GeoPackage
cargo test -p meerschaum-sql --features duckdb     # + DuckDB
cargo test -p meerschaum-sql --features oracle     # + Oracle
```

`tests/driver_native.rs` exercises the MSSQL / DuckDB / Oracle drivers directly. MSSQL's
instance schema must exist (v4 does not create it either); add `?schema=<name>` to the URI to
use one other than `dbo`.
