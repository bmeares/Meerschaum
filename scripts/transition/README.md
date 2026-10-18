# v4 -> v5 transition harness

Unit and parity tests compare v4.1.4 and v5 side by side. This harness does what a user does on upgrade: it
takes state that **released meerschaum 4.1.4 created** and continues it with the working tree's v5, on the
same root and the same databases, then compares.

```bash
scripts/transition/run.sh                                   # sqlite
scripts/transition/run.sh --flavors sqlite,postgis,timescaledb,mssql,mysql,mariadb,oracle
scripts/transition/run_all.py run-mssql mssql --only ddl    # one flavor, selected stages
scripts/transition/run.sh --flavors sqlite --wheel path/to/meerschaum-5.*.whl
```

Needs `uv`, network access for PyPI, the built extension (`scripts/rust/build_py.sh`) and, for the standalone
binary checks, `target/debug/mrsm`. The server flavors use the disposable containers of
`tests/docker-compose.yaml`; the harness only touches objects named `trn*` (schema `trn` on PostgreSQL
flavors, database `trndb` on MySQL / MariaDB / MSSQL, common user `C##TRN` on Oracle) and drops them afterwards. Nothing is written inside the
repository: the scratch directory is `MRSM_TRANSITION_DIR` (default `~/.cache/mrsm-transition`).

Run **one run per flavor at a time**: the objects the harness creates have fixed names (`trn*`, job names carry a
short flavor tag), so two runs of one flavor (or a run next to another agent's) collide.

## What it builds with v4

A user's interpreter is a venv holding only `pip install meerschaum==4.1.4` (v4 installs its ~110 packages
into `<root>/venvs/mrsm` on first run, exactly as for a user). Through v4's own CLI and Python API, on a fresh root:

- config written by v4: a custom `sql:` connector per flavor, an `MRSM{...}` reference, an `MRSM_SQL_<LABEL>`
  environment connector and a `postgresql://` environment connector that is never dialed;
- pipes with every dtype family (int, float, bool, str, naive and tz-aware datetime, date, json, uuid,
  numeric, bytes, geometry on postgis), `columns`, `indices`, `upsert`, `tags`, `autoincrement`,
  `null_indices`, `static`, a SQL `fetch.definition` reading another pipe through `{{ Pipe() }}`, two
  plugin-fetch pipes, inserts, updates and upserts;
- a v4-style plugin with `required` (so v4 creates its venv), `fetch`, `@make_action`,
  `add_plugin_argument`, pre/post-sync hooks and `@api_plugin`; a second plugin doing a plain `import pandas`;
- users (admin and regular), tokens with scopes, a registered plugin;
- jobs (one finished, one stopped `--loop`, one **left running** when v5 takes over) and a compose project that
  is `up`.

## What it checks with v5

`stage_v5.py`, each check recorded as `PASS`, `FAIL` (a regression), `EXPECTED` (a difference recorded in
`crates/meerschaum-py/PARITY.md`, with its row id in the detail: dtype backends W17D2-006, a PostgreSQL view that
v4's `read()` rolled back W17O-007a), `DIFF` (a difference still to classify) or `INFO`:

| Area | Checks |
|---|---|
| config | `get_config()` identical under v4 and v5 on v4's files; a v5 `edit config` changes only the edited lines and v4 reads the result |
| pipes | `get_data()` / dtypes / rowcount / sync time / parameters / column types equal v4's on the same tables, before and after v5 writes; incremental `sync` continues without duplicates or lost rows; `edit` / `update_parameters`; `copy`, `verify`, `clear`, `drop`, `delete` on a copy |
| cli | the same `show ...` commands under both, output diffed, timings |
| python-api | one script written against the v4 API (`get_pipes`, `get_connector`, `Pipe`, `get_data(params=)`, `SQLConnector.read/exec/value/to_sql/engine`, `mrsm.entry`, sync unpacking) must give the same JSON |
| plugins | v4 plugin venv reused untouched, actions (both `a_b` and `a b` spellings), flags, hooks, a plugin that imports a package from the shared venv |
| ddl | `mrsm sql <label> "<DDL>"` (views, tables, multi-statement): outcome and resulting objects vs v4 |
| engine | `sync pipes --parents` on a root v4 created; engine state must land in `_mrsm_internal` |
| api | a v4 server and a v5 server on the same instance: login, JWT and token routes, scopes, an `@api_plugin` route; v4 client against the v5 server and v5 client against the v4 server |
| env-uri / dash | (first flavor only) `MRSM_SQL_MAIN` and a custom label set to URIs follow the URI like v4 (the default host is patched to a dead port, so a client that ignores the URI fails harmlessly); a v4-style Dash plugin page and callback served by `mrsm start api` |
| jobs | v5 lists the three v4 jobs with the right status, reads the running job's log, stops the v4 daemon by its pid file, starts a v4 job under its own supervisor; v4 reads the state v5 wrote; nothing is left running |
| compose | `ps` / `explain` / `up --dry` / `down` / `up` on the v4-started project |
| pip (`--wheel`) | `pip install -U <wheel>` over 4.1.4 leaves no v4 module behind, the console scripts run v5, the base wheel declares no dependencies, the `api`/`dash` extras are enough to import `meerschaum.api` with `MRSM_NO_AUTO_INSTALL=1`; then sqlite is rerun with the installed wheel |

Run directories are under `<scratch>/work/run-<flavor>/` (`out/findings.json`, the v4 / v5 server logs,
snapshots). `report.py` prints every non-`PASS` finding and exits 1 on a `FAIL`.

## Isolation

Every process (v4 and v5) gets an explicit scratch `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR`, and a scratch sqlite
default instance (`MRSM_SQL_TRN` and `MRSM_CONFIG` pointing `meerschaum.instance` at it) so that no command can
resolve `sql:main`. The environment is built from scratch (`env -i` style): v4 reads every `MRSM_<TYPE>_<LABEL>`
variable as a connector. Never point the harness at a real root.

## Files

`common.py` (environment and runners), `flavors.py`, `data.py` (synthetic frames), `v4_*.py` (run by the v4
interpreter), `snapshot.py` / `compare.py` (version-agnostic pipe snapshots), `v5_continue.py`,
`pyapi_script.py`, `api_*.py`, `stage_v4.py` / `stage_v5.py` / `stage_pip.py`, `plugin/` (the synthetic plugins),
`run_all.py` (one flavor), `run_pip.py`, `report.py`, `run.sh`.
