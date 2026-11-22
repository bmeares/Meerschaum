# Running tests

What the test harnesses need that their errors do not tell you. The isolation rules in
[`CLAUDE.md`](../../CLAUDE.md) apply to every command here.

## Rust

- Integration tests are suite binaries: one `tests/<suite>/main.rs` per area, each file a module.
  Run one module with `cargo test -p <crate> --test <suite> -- <module>::`; list the suites with
  `ls crates/<crate>/tests`. `scripts/check_architecture.py` caps the number of test binaries:
  add a module to an existing suite; a test binary of its own is for tests that change process-wide
  state (environment variables, signal handlers) that would leak into a shared suite.
- Database-backed tests skip unless their service URI is set (an empty value counts as set):
  `MRSM_TEST_PG_URI`, `MRSM_TEST_TIMESCALE_URI`, `MRSM_TEST_POSTGIS_URI`,
  `MRSM_TEST_TIMESCALE_HA_URI` (`timescaledb-ha://`), `MRSM_TEST_MYSQL_URI`,
  `MRSM_TEST_MARIADB_URI`, `MRSM_TEST_MSSQL_URI`, `MRSM_TEST_ORACLE_URI` (`--features oracle`,
  needs Instant Client), `MRSM_TEST_GPKG_DIR`, `MRSM_TEST_DUCKDB_PATH` / `MRSM_TEST_DUCKDB_LIB`,
  `MRSM_TEST_VALKEY_URI`, `MRSM_TEST_MQTT_URI`. `MRSM_TEST_PYTHON` picks the reference interpreter
  for cross-implementation tests; `MRSM_TEST_KEEP=1` keeps their scratch state.
- The databases come from `tests/docker-compose.yaml`; host ports are in `tests/connectors.py` and
  `crates/meerschaum-connector-sql/README.md`.
- `MRSM_DUCKDB_LIB` pins the native DuckDB library for a process (otherwise it is found in
  `<root>/drivers`, the system loader, or downloaded on first use); read the connector README
  before changing DuckDB loading.
- Team-executor, Docker and confined-compiler tests have their own gates:
  [`teams-and-executors.md`](teams-and-executors.md) "Tests".

## Python

- Tests that need the extension fail, never skip, when it is not built
  (`scripts/rust/build_py.sh --profile ci`).
- A few tests use a standalone `mrsm`: `MRSM_TEST_BIN`, else the newest
  `target/{release,ci,debug}/mrsm` outside CI; `tests/_binary.py` checks its version and age.
- `tests/conftest.py` points `sql:main` at a SQLite file under the test root unless
  `MRSM_SQL_MAIN` is already set.
- **Flavor keys are not SQL flavor names**: `timescaledb`, `timescaledb_ha`, `postgis`, `mariadb`,
  `mysql`, `mssql`, `oracle`, `sqlite`, `api`, `valkey` (no `postgresql`). `scripts/test.sh`
  defaults to `api,timescaledb`, needs `pytest-xdist` and `pytest-cache`, and exits before pytest
  on an unknown key.
- Oracle recordings (`tests/oracle/*.json`) replay the deleted parity-phase Python mode and cannot
  be re-recorded; pandas-major-dependent tests list themselves in `pandas_dependent`.
- `tests/console` (Playwright, opt-in with `MRSM_TEST_CONSOLE=1`) starts real `mrsm start api`
  servers on random ports with the `w7_*` plugins; it needs `target/debug/mrsm`,
  `meerschaum[dash,sql,pandas]` and Chromium (`tests/console/build_native.sh` builds the native
  page plugin). `tests/api_host` runs in process.
