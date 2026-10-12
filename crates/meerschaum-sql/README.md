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
| MSSQL | `mssql` | `tiberius` | not yet (`SqlError::DriverUnavailable`) |
| Oracle | `oracle` | `oracle` | not yet |
| DuckDB | `duckdb` | `duckdb` | not yet |

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
