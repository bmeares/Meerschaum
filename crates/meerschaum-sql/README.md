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

Arrow fields carry `mrsm:db_type` (the driver's type name) and, for special columns,
`mrsm:dtype` (`json`, `bytes`, `uuid`, `geometry`, `numeric`) and `mrsm:encoding` (`ewkb` for
PostGIS, `mysql` for MySQL geometry).

## Golden fixtures

`tests/fixtures/queries.json` (about 21k cases: every flavor and pipe shape, replayed byte for
byte by `tests/golden.rs`) and `tests/fixtures/uris.json` are generated from the Python implementation in this repository:

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_queries.py
PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
    ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_uris.py
```

Use empty temporary directories for `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR` so no plugins or
real configuration are loaded; all fixture inputs are synthetic.

## Running the driver integration tests

The SQLite tests (`tests/driver_sqlite.rs`) always run. The PostgreSQL and MySQL tests skip
unless their URIs are set; they create uniquely named `_mrsm_rs_test_*` tables and drop them.

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
service instead of MariaDB. Any PostgreSQL-family URI works for `MRSM_TEST_PG_URI`, including
the `timescaledb://` / `timescaledb-ha://` / `postgis://` schemes (e.g.
`timescaledb-ha://test:test1234@localhost:5459/testdb`).
