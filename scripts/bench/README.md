# Release performance checks

Run this harness on a quiet machine after the correctness gates pass. It compares released
v4.1.4 in `.venv-ref` with the installed v5 wheel and the standalone release binary. Existing
results remain the historical baseline; the runner never changes `.venv-ref` or its venvs.

Prepare the release binary and a wheel from the same source revision. Install that wheel into
a dedicated environment below `~/.cache`, with the data and SQL extras available. Pass that
environment's Python with `--v5-python`; its `bin/mrsm` must be the wheel's console script.
An editable installation or a package imported from the repository is refused. Dependencies
may be supplied by a `.pth` file pointing at an existing test environment's site-packages,
after installing the wheel into the benchmark environment with `--no-deps`. That shares
dependency files without replacing the test environment's Meerschaum installation. Do not
change the wheel environment during a benchmark run.

Before a PostGIS or TimescaleDB matrix, provision `psycopg` and a working libpq implementation
in both Python environments (for example `psycopg[binary]`). `psycopg2` alone does not satisfy
the default `postgresql+psycopg` engine. Verify a short sync with every implementation on both
selected servers before starting the full matrix; preparation is outside measured timings.

Prepare installed bytecode equally in both environments. Pip normally compiles bytecode;
`--no-compile` skips it. If caches are missing, run each environment's `python -m compileall -q`
on its installed `meerschaum` package, newly provisioned driver packages, and the v5
`_meerschaum_mrsm.py` entry module before timing. Timing children use `-B`, which reads existing
caches but does not create missing ones. Keep those prepared caches unchanged during the run.

The runner checks the console script's selected Python, entry point, installed RECORD hash
and version. It also checks the native extension against RECORD and records its SHA-256,
the standalone binary SHA-256, interpreter versions and dependency versions. Both Python
environments must use the same major/minor interpreter version. The driver revision is
recorded separately; an installed artifact is identified by its own hash.

```bash
python scripts/bench/run.py --plan --label rc1
python scripts/bench/run.py --label rc1 --v5-python "$HOME/.cache/mrsm-bench-wheel/bin/python" \
    --backends sqlite,postgis,timescaledb --sizes 10k,100k,1M \
    --cases sync,inc,filter_existing,get_data,chain,latency,startup --repeats 3
```

The plan command only prints the workload. It imports no Meerschaum package, runs no child
process and accesses no database. The measured run records load averages and the number of
`rustc` processes before startup measurements and each backend, and refuses active Rust
compilation. The operator must also ensure no correctness suite or other benchmark is running.

The five implementation columns have these meanings:

| Column | Execution |
|---|---|
| `v4py` | `.venv-ref` Python API, time around the call |
| `v4cli` | `.venv-ref` `python -m meerschaum`, whole process |
| `v5py` | Installed wheel Python API, time around the call |
| `v5cli` | Installed wheel `mrsm` console script, whole process |
| `v5bin` | Snapshot of `target/release/mrsm`, whole process |

Historical reports called the standalone binary `v5cli`. The wave 10 comparison maps that
column to `v5bin`; it does not pretend there is an older wheel-console measurement. The
removed Python-core mode is not an implementation choice.

Each backend measures fresh sync at 10k, 100k and 1M rows; incremental sync at 100k and 1M
with 1% appended rows; Python `filter_existing` at 100k and `get_data` at 1M; CLI three-pipe
chains; and Python per-call latency. Both v5 CLIs also run the dependency-aware chain. Startup
cases measure fresh Python imports, CLI `--version` and CLI `show version`. Every measured
case, including 1M, gets the requested three repeats. Sync and incremental cases up to 100k
get one additional warm-up; `get_data`, `filter_existing`, chains and startup also warm up.

SQLite uses fresh files below the run's cache directory. PostGIS uses the test server on
127.0.0.1:5449; TimescaleDB uses 127.0.0.1:5439. Each receives a newly created
`w17cbench_<random>` database and schema. A name collision fails without dropping the existing
database. Only a database successfully created by this run is removed, including on ordinary
errors and terminal interruption. `--keep` retains local scratch files; server databases are
still removed. `--workdir` must name an empty directory below `~/.cache`. There is no Docker
management or connection to the default SQL instance. MySQL is not implemented by this runner.

Results are written to `results/<date>-<label>.json` and `.md`, with paths and URI credentials
removed from errors. Missing implementations and partial failures return nonzero; inspect the
failure section even when a median is available. An interrupted or failed rerun preserves an
existing completed result under the same label. Keep stdout/stderr separately when investigating
a failed run; the public result intentionally removes local paths.

After startup and each backend, a separate atomic checkpoint retains cumulative raw samples
and identity metadata. Checkpoints have `complete=false`; they do not prove final workload or
cleanup acceptance and are not automatically resumed or merged into a completed result.

Runs under one label merge only when artifact hashes, interpreter/dependency versions,
harness hash, sizes, cases, implementations and repeat counts agree. A previous incomplete
report or an incompatible merge is refused and saved separately. Completion is checked over
every retained measurement. Dots in labels are preserved as part of the filename.

Any v5 median more than 10% slower than the corresponding v4 path needs three additional
repeats and profiling. Re-run the relevant backend, implementation and case under a separate
label so the full result remains intact. `--trace` deliberately enables `MRSM_CORE_TRACE=1`
in the v5 child environment for profiling; timing runs clear inherited `MRSM_*` settings. The
report also flags matched v5 cases more than 20% slower than wave 10. Confirm these separately;
the runner does not declare performance acceptance from incomplete results.

Based on the recorded wave 10 medians, allow about 75–120 minutes for the full three-backend,
five-implementation workload, plus release artifact preparation and any confirmation runs.
The v4 SQLite 1M sync dominates this estimate. This is a planning estimate, not a measured
rc1 result.

Static harness checks require only the standard library and do not run benchmarks:

```bash
python -m unittest discover -s scripts/bench -p test_harness.py
python -m py_compile scripts/bench/defs.py scripts/bench/run.py scripts/bench/worker.py
scripts/parity/check_fixture_hygiene.sh scripts/bench
```
