#!/usr/bin/env python3
"""
Fixed sync benchmark set: Meerschaum v4.1.4 (the reference venv, `scripts/parity/setup_reference.sh`)
against v5 (the installed wheel's Python API and console script, and the release `mrsm` binary).
Not part of CI. Spec §14 "Benchmarks": regressions block the RC.

    cargo build --release -p meerschaum --bin mrsm     # target/release/mrsm
    python scripts/bench/run.py --label rc1 --v5-python <wheel-venv>/bin/python
    python scripts/bench/run.py --plan              # no imports, subprocesses or databases

Implementations (columns of the report):

    v4py   v4.1.4, `pipe.sync()` etc. from Python, timed around the call (no interpreter startup)
    v4cli  v4.1.4, `python -m meerschaum sync pipes ...`, timed as a whole process
    v5py   installed wheel from --v5-python, timed around Python API calls
    v5cli  wheel's console-script mrsm beside --v5-python, timed as a whole process
    v5bin  standalone target/release/mrsm, timed as a whole process

Backends: SQLite files, PostGIS on test port 5449 and TimescaleDB on test port 5439.
Each server backend gets a fresh database and schema starting with w17cbench_.

    sync_{10k,100k,1M}   fresh destination, source SQL pipe (`sql:a_src` -> `sql:b_dest`)
    inc_{100k,1M}        destination already synced; 1% new source rows; time the re-sync
    filter_existing_100k Python only: `pipe.filter_existing` of a 100k frame (1% new)
    get_data_1M          Python only: `pipe.get_data()` of 1M rows
    chain_3              CLI only: `sync pipes -w 1` over raw -> mid -> leaf `{{ Pipe() }}` chain
    chain_3_engine       both v5 CLIs: `sync pipes --parents` on the leaf of the same chain
    startup_*            Python import, CLI --version and CLI show version, fresh processes
    lat_*                Python only (case `latency`): per-call latency on a 50-row pipe, one
                         process per repeat: `lat_first_call` (the first `get_rowcount()` of
                         the process), warm `lat_get_sync_time` / `lat_get_rowcount` /
                         `lat_get_data` (median of `--latency-iterations` calls), and a new `Pipe`
                         per call for `lat_fresh_get_sync_time` and for the `{{ Pipe() }}` chain
                         leaf (`lat_template_get_sync_time`)

Every run uses a fresh temporary MRSM_ROOT_DIR per implementation, an empty plugins dir, and
scratch directories below ~/.cache. No reference venv is modified. One warm-up (a 10k sync) per implementation and backend,
then one extra warm-up repeat per case for sizes up to 100k, then `--repeats` timed repeats
(including all three at 1M). The machine must be quiet; concurrent Rust compilation is refused.

Output: scripts/bench/results/<date>-<label>.{json,md}. No hostnames, paths or environment are
recorded.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import platform
import re
import signal
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE))
import defs  # noqa: E402

REF_PY = REPO / '.venv-ref' / 'bin' / 'python'
MRSM_BIN = REPO / 'target' / 'release' / 'mrsm'
V5_PY = Path(sys.executable)
PROFILE_TRACE = False
RUN_TIMEOUT = 3600


class Db:
    """Raw SQL access to one backend, used only to prepare and verify (never timed)."""

    def __init__(self, backend: str, workdir: Path):
        self.backend = backend
        self.workdir = workdir
        self.database = defs.PG_DATABASE
        self.created_database = False

    def prepare(self) -> None:
        """Create an exclusively owned scratch database; never drop a pre-existing name."""
        if self.backend == 'sqlite':
            return
        if not re.fullmatch(r'w17cbench_[a-z0-9_]{1,40}', self.database):
            raise ValueError('Refusing a non-scratch database name')
        self._psql(f'CREATE DATABASE {self.database}', database='testdb')
        self.created_database = True
        self.exec(f'CREATE EXTENSION IF NOT EXISTS {self.backend}')
        self.exec(f'CREATE SCHEMA {defs.SCHEMA}')

    def cleanup(self) -> None:
        if self.created_database:
            self._psql(f'DROP DATABASE {self.database} WITH (FORCE)', database='testdb')
            self.created_database = False

    def tbl(self, name: str) -> str:
        return name if self.backend == 'sqlite' else f'{defs.SCHEMA}.{name}'

    def exec(self, sql: str, which: str = 'dest') -> None:
        if self.backend == 'sqlite':
            path = self.workdir / ('src.db' if which == 'src' else 'dest.db')
            con = sqlite3.connect(path, timeout=120)
            try:
                con.execute(sql)
                con.commit()
            finally:
                con.close()
        else:
            self._psql(sql)

    def scalar(self, sql: str, which: str = 'dest'):
        if self.backend == 'sqlite':
            path = self.workdir / ('src.db' if which == 'src' else 'dest.db')
            con = sqlite3.connect(path, timeout=120)
            try:
                row = con.execute(sql).fetchone()
            finally:
                con.close()
            return None if row is None else row[0]
        out = self._psql(sql, tuples=True)
        return out.strip() if out.strip() else None

    def count(self, table: str, which: str = 'dest') -> Optional[int]:
        try:
            value = self.scalar(f'SELECT COUNT(*) FROM {table}', which)
        except Exception:
            return None
        return None if value is None else int(value)

    def drop(self, table: str, which: str = 'dest') -> None:
        self.exec(f'DROP TABLE IF EXISTS {table}', which)

    def _psql(self, sql: str, tuples: bool = False, database: Optional[str] = None) -> str:
        cmd = [
            'psql', '-h', '127.0.0.1', '-p', str(defs.PG_BACKENDS[self.backend]),
            '-U', 'test', '-d', database or self.database,
            '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-c', sql,
        ]
        if tuples:
            cmd += ['-t', '-A']
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('PG') and key not in ('TMUX', 'TMUX_PANE')}
        proc = subprocess.run(
            cmd, env={**env, 'PGPASSWORD': 'test1234'}, capture_output=True, text=True,
            timeout=120,
        )
        if proc.returncode != 0:
            raise RuntimeError(f'psql failed: {proc.stderr.strip()}')
        return proc.stdout

    # Row generators ------------------------------------------------------------------------

    def create_rows(self, table: str, start: int, stop: int, create: bool, which: str = 'src') -> None:
        """Rows i in [start, stop): dt = base + i s, id = i % 50, value = i * 0.5."""
        base = defs.BASE_EPOCH
        if self.backend == 'sqlite':
            if create:
                self.exec(
                    f'CREATE TABLE {table} (dt TIMESTAMP, id BIGINT, value FLOAT)', which,
                )
                self.exec(f'CREATE INDEX ix_{table}_dt ON {table} (dt)', which)
            self.exec(
                f"WITH RECURSIVE s(i) AS (SELECT {start} UNION ALL SELECT i + 1 FROM s "
                f"WHERE i < {stop - 1}) INSERT INTO {table} "
                f"SELECT strftime('%Y-%m-%d %H:%M:%S', {base} + i, 'unixepoch') || '.000000', "
                f"i % 50, i * 0.5 FROM s", which,
            )
        else:
            if create:
                self.exec(
                    f'CREATE TABLE {table} (dt TIMESTAMPTZ, id BIGINT, value DOUBLE PRECISION)',
                )
                self.exec(f'CREATE INDEX ix_{table.split(".")[-1]}_dt ON {table} (dt)')
            self.exec(
                f"INSERT INTO {table} SELECT to_timestamp({base}) + i * interval '1 second', "
                f"i % 50, i * 0.5 FROM generate_series({start}, {stop - 1}) AS i"
            )


class Impl:
    """How to launch one implementation."""

    def __init__(self, name: str, workdir: Path, backend: str, snapshot: Optional[Path] = None):
        self.name = name
        self.snapshot = snapshot  # copy of the v5 package and binary, immune to concurrent rebuilds
        self.workdir = workdir
        self.backend = backend
        self.root = workdir / f'root_{name}'
        self.plugins = workdir / 'plugins'
        self.root.mkdir(parents=True, exist_ok=True)
        self.plugins.mkdir(exist_ok=True)

    @property
    def is_py(self) -> bool:
        return self.name in defs.PY_IMPLS

    def available(self) -> Optional[str]:
        if self.name.startswith('v4') and not REF_PY.exists():
            return 'missing .venv-ref (run scripts/parity/setup_reference.sh)'
        if self.name == 'v5bin' and not self.mrsm_bin().exists():
            return 'missing target/release/mrsm (cargo build --release -p meerschaum --bin mrsm)'
        if self.name.startswith('v5') and not V5_PY.exists():
            return 'missing --v5-python (install the built wheel in a dedicated environment)'
        if self.name == 'v5cli' and not self.wheel_cli().exists():
            return 'missing mrsm console script beside --v5-python (install the built wheel)'
        return None

    def mrsm_bin(self) -> Path:
        return self.snapshot / 'mrsm' if self.snapshot else MRSM_BIN

    def wheel_cli(self) -> Path:
        return V5_PY.parent / 'mrsm'

    def env(self) -> Dict[str, str]:
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('MRSM_', 'PYTHON')) and k not in ('TMUX', 'TMUX_PANE')}
        env.update({
            'MRSM_ROOT_DIR': str(self.root),
            'MRSM_PLUGINS_DIR': str(self.plugins),
            'MRSM_VENVS_DIR': str(self.root / 'venvs'),
            'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': defs.DEST_INSTANCE}}),
            'MRSM_NO_AUTO_INSTALL': '1',
            'MRSM_PYTHON': str(REF_PY if self.name.startswith('v4') else V5_PY),
            'MRSM_NO_UPDATE_CHECK': '1',
            'UV_PYTHON': str(REF_PY if self.name.startswith('v4') else V5_PY),
            'PYTHONHASHSEED': '0',
            'PYTHONDONTWRITEBYTECODE': '1',
            'PYTHONNOUSERSITE': '1',
        })
        env.update(defs.connector_env(self.backend, str(self.workdir)))
        if PROFILE_TRACE and self.name.startswith('v5'):
            env['MRSM_CORE_TRACE'] = '1'
        env['MRSM_BENCH_PG_SCHEMA'] = defs.SCHEMA
        env['MRSM_BENCH_PG_DATABASE'] = defs.PG_DATABASE
        return env

    def py_cmd(self, *args: str) -> List[str]:
        worker = str(HERE / 'worker.py')
        if self.name.startswith('v4'):
            return [str(REF_PY), '-B', '-s', '-E', worker, *args]
        return [str(V5_PY), '-B', '-s', worker, *args]

    def cli_cmd(self, *args: str) -> List[str]:
        if self.name == 'v4cli':
            return [str(REF_PY), '-B', '-s', '-E', '-m', 'meerschaum', *args]
        if self.name == 'v5cli':
            return [str(self.wheel_cli()), *args]
        return [str(self.mrsm_bin()), *args]

    def clear_cache(self) -> None:
        shutil.rmtree(self.root / 'cache', ignore_errors=True)

    def run(self, cmd: List[str]) -> subprocess.CompletedProcess:
        # cwd is the work dir so that neither the repository nor ~/.local shadows the package.
        return subprocess.run(
            cmd, env=self.env(), cwd=self.workdir, capture_output=True, text=True,
            timeout=RUN_TIMEOUT,
        )

    def worker(self, *args: str) -> Dict[str, Any]:
        proc = self.run(self.py_cmd(*args))
        for line in reversed(proc.stdout.splitlines()):
            if line.startswith('RESULT '):
                result = json.loads(line[len('RESULT '):])
                if proc.returncode:
                    return {**result, 'ok': False,
                            'error': f'worker exited {proc.returncode}: {(proc.stderr or proc.stdout)[-400:]}'}
                return result
        return {'ok': False, 'error': (proc.stderr or proc.stdout)[-400:]}

    def timed_cli(self, *args: str) -> Dict[str, Any]:
        start = time.perf_counter()
        proc = self.run(self.cli_cmd(*args))
        seconds = time.perf_counter() - start
        return {
            'ok': proc.returncode == 0, 'seconds': seconds,
            'error': '' if proc.returncode == 0 else (proc.stderr or proc.stdout)[-400:],
        }

    def sync_timed(self, connector: str, metric: str, instance: str = defs.DEST_INSTANCE):
        """Time one `sync` of a registered pipe."""
        if self.is_py:
            return self.worker('sync', connector, metric, instance)
        return self.timed_cli('sync', 'pipes', '-c', connector, '-m', metric, '-i', instance)


class Bench:
    def __init__(self, args: argparse.Namespace, workdir: Path):
        self.args = args
        self.workdir = workdir
        self.results: List[Dict[str, Any]] = []
        self.notes: List[str] = []
        self.loads: List[Dict[str, Any]] = []
        self.databases: List[Db] = []
        self.snapshot = self._snapshot()

    def _snapshot(self) -> Path:
        """Freeze the release binary; the installed wheel environment stays unchanged during runs."""
        snap = self.workdir / 'v5snapshot'
        if snap.exists():
            return snap
        snap.mkdir(parents=True)
        if MRSM_BIN.exists():
            shutil.copy2(MRSM_BIN, snap / 'mrsm')
        return snap

    # -- helpers ----------------------------------------------------------------------------

    def log(self, msg: str) -> None:
        print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)

    def check_load(self, backend: str) -> None:
        """Refuse concurrent Rust compilation and record load without machine identifiers."""
        load = list(os.getloadavg()) if hasattr(os, 'getloadavg') else []
        rustc = None
        if shutil.which('pgrep'):
            proc = subprocess.run(['pgrep', '-xc', 'rustc'], capture_output=True, text=True,
                                  timeout=10)
            if proc.returncode in (0, 1):
                rustc = int(proc.stdout.strip() or '0')
        self.loads.append({'backend': backend, 'load_average': load, 'rustc_processes': rustc})
        self.log(f'{backend}: load averages={load}, rustc processes={rustc}')
        if shutil.which('uptime'):
            proc = subprocess.run(['uptime'], capture_output=True, text=True, timeout=10)
            self.log(proc.stdout.strip())
        if rustc:
            raise RuntimeError('Rust compilation is active; run benchmarks after the machine is quiet')

    def startup(self) -> None:
        """Fresh-process Python import and both CLI version paths, using isolated roots."""
        self.check_load('startup')
        for name in self.args.impls:
            impl = Impl(name, self.workdir / 'startup', 'sqlite', self.snapshot)
            self._unavailable(impl)
            if impl.is_py:
                python = REF_PY if name.startswith('v4') else V5_PY
                flags = ['-B', '-s', '-E'] if name.startswith('v4') else ['-B', '-s']
                commands = {'startup_import': [str(python), *flags, '-c', 'import meerschaum']}
            else:
                commands = {
                    'startup_version': impl.cli_cmd('--version'),
                    'startup_show_version': impl.cli_cmd('show', 'version'),
                }
            for case, cmd in commands.items():
                runs = []
                for k in range(1 + self.args.repeats):
                    start = time.perf_counter()
                    proc = impl.run(cmd)
                    result = {'ok': proc.returncode == 0, 'seconds': time.perf_counter() - start,
                              'error': '' if proc.returncode == 0 else proc.stderr[-400:]}
                    if k > 0 or not result['ok']:
                        runs.append(result)
                self.record('startup', case, name, runs)

    def record(self, backend, case, impl, runs, expected_note=''):
        good = [r['seconds'] for r in runs if r.get('ok') and 'seconds' in r]
        entry = {
            'backend': backend, 'case': case, 'impl': impl,
            'seconds': [round(s, 6) for s in good],
            'median': round(statistics.median(good), 6) if good else None,
            'min': round(min(good), 6) if good else None,
            'failed': len(runs) - len(good),
        }
        errs = [r.get('error') or r.get('message') for r in runs if not r.get('ok')]
        if errs:
            entry['error'] = errs[0]
        self.results.append(entry)
        shown = f'median {entry["median"]}s' if good else f'FAILED: {entry.get("error")}'
        self.log(f'  {backend} {case} {impl}: {shown}')

    def reps(self, label: str):
        warm = 1 if defs.SIZES[label] <= 100_000 else 0
        return warm, self.args.repeats

    # -- one backend ------------------------------------------------------------------------

    def run_backend(self, backend: str) -> None:
        self.check_load(backend)
        a = self.args
        wd = self.workdir / backend
        wd.mkdir(parents=True, exist_ok=True)
        db = Db(backend, wd)
        self.databases.append(db)
        labels = [lab for lab in defs.SIZES if lab in a.sizes]
        needed = sorted(set(labels) | {'10k', '100k'}, key=lambda s: defs.SIZES[s])
        impls = [Impl(n, wd, backend, self.snapshot) for n in a.impls]
        impls = [i for i in impls if not self._unavailable(i)]
        if not impls:
            return
        self.log(f'== {backend}: preparing sources {needed}; scratch database {db.database}')
        db.prepare()
        for lab in needed:
            db.create_rows(db.tbl(f'src_{lab}'), 0, defs.SIZES[lab], create=True)
        if 'latency' in a.cases:
            db.create_rows(db.tbl('src_small'), 0, defs.LATENCY_ROWS, create=True)
        for impl in impls:
            for lab in labels:
                if 'inc' in a.cases:
                    t = db.tbl(f'src_inc_{lab}_{impl.name}')
                    db.create_rows(t, 0, defs.SIZES[lab], create=True)
        self.log('registering pipes (v4.1.4)')
        if not REF_PY.exists():
            raise SystemExit('The reference venv is required to register the benchmark pipes.')
        reg = Impl('v4py', wd, backend, self.snapshot)
        out = reg.worker(
            'setup', json.dumps(defs.pipe_definitions(backend, labels, impls=defs.ALL_IMPLS)),
        )
        if not out.get('ok'):
            raise SystemExit(f'pipe registration failed: {out}')

        for impl in impls:
            self.log(f'-- {backend} / {impl.name}: warm-up')
            self._warmup(db, impl)
            if 'sync' in a.cases:
                for lab in labels:
                    self.case_sync(db, impl, lab)
            if 'inc' in a.cases:
                for lab in labels:
                    if lab != '10k':
                        self.case_inc(db, impl, lab)
            if impl.is_py and 'filter_existing' in a.cases and '100k' in labels:
                self.case_filter_existing(db, impl)
            if impl.is_py and 'get_data' in a.cases and '1M' in labels:
                self.case_get_data(db, impl)
            if not impl.is_py and 'chain' in a.cases:
                self.case_chain(db, impl)
            if impl.is_py and 'latency' in a.cases:
                self.case_latency(db, impl)
        if 'chain' in a.cases:
            for impl in impls:
                if impl.name in ('v5cli', 'v5bin'):
                    self.case_chain(db, impl, engine=True)
        db.cleanup()

    def _unavailable(self, impl: Impl) -> bool:
        why = impl.available()
        if why:
            raise RuntimeError(f'{impl.name}: {why}')
        return bool(why)

    def _reset(self, db: Db, impl: Impl, metric: str) -> None:
        db.drop(db.tbl(defs.target_name(metric)))
        impl.clear_cache()

    def _warmup(self, db: Db, impl: Impl) -> None:
        metric = defs.sync_metric('warmup', impl.name)
        self._reset(db, impl, metric)
        res = impl.sync_timed(defs.SRC_INSTANCE, metric)
        self.log(f'   warm-up sync(10k): ok={res.get("ok")} {res.get("seconds", 0):.2f}s')
        self._reset(db, impl, metric)

    def _run_timed(self, impl: Impl, fn, check, warm: int, reps: int):
        """Run `fn` (warm + reps) times; `check` verifies a run. The warm-up runs are dropped."""
        runs = []
        for k in range(warm + reps):
            res = fn(k)
            if res.get('ok') and not check(k):
                res = {**res, 'ok': False, 'error': 'row count verification failed'}
            if k >= warm:
                runs.append(res)
            elif not res.get('ok'):
                runs.append(res)
        return runs

    # -- cases ------------------------------------------------------------------------------

    def case_sync(self, db: Db, impl: Impl, lab: str) -> None:
        n = defs.SIZES[lab]
        metric = defs.sync_metric(lab, impl.name)
        table = db.tbl(defs.target_name(metric))
        warm, reps = self.reps(lab)

        def fn(_k):
            self._reset(db, impl, metric)
            return impl.sync_timed(defs.SRC_INSTANCE, metric)

        runs = self._run_timed(impl, fn, lambda _k: db.count(table) == n, warm, reps)
        self.record(db.backend, f'sync_{lab}', impl.name, runs)

    def case_inc(self, db: Db, impl: Impl, lab: str) -> None:
        n = defs.SIZES[lab]
        step = n // 100
        metric = defs.inc_metric(lab, impl.name)
        table = db.tbl(defs.target_name(metric))
        src = db.tbl(f'src_inc_{lab}_{impl.name}')
        self._reset(db, impl, metric)
        seed = impl.sync_timed(defs.SRC_INSTANCE, metric)
        if not seed.get('ok') or db.count(table) != n:
            self.record(db.backend, f'inc_{lab}', impl.name, [{**seed, 'ok': False, 'error': 'seed failed'}])
            return
        warm, reps = self.reps(lab)
        state = {'rows': n}

        def fn(_k):
            db.create_rows(src, state['rows'], state['rows'] + step, create=False)
            state['rows'] += step
            return impl.sync_timed(defs.SRC_INSTANCE, metric)

        runs = self._run_timed(impl, fn, lambda _k: db.count(table) == state['rows'], warm, reps)
        self.record(db.backend, f'inc_{lab}', impl.name, runs)

    def _ensure_seed(self, db: Db, impl: Impl, lab: str) -> bool:
        metric = defs.sync_metric(lab, impl.name)
        table = db.tbl(defs.target_name(metric))
        if db.count(table) == defs.SIZES[lab]:
            return True
        self._reset(db, impl, metric)
        res = impl.sync_timed(defs.SRC_INSTANCE, metric)
        return bool(res.get('ok')) and db.count(table) == defs.SIZES[lab]

    def case_filter_existing(self, db: Db, impl: Impl) -> None:
        metric = defs.sync_metric('100k', impl.name)
        if not self._ensure_seed(db, impl, '100k'):
            self.record(db.backend, 'filter_existing_100k', impl.name, [{'ok': False, 'error': 'seed failed'}])
            return
        runs = []
        for k in range(1 + self.args.repeats):
            res = impl.worker('filter_existing', defs.SRC_INSTANCE, metric, defs.DEST_INSTANCE, '100000')
            if res.get('ok') and res.get('unseen') != 1000:
                res = {**res, 'ok': False, 'error': f'expected 1000 unseen, got {res.get("unseen")}'}
            if k >= 1 or not res.get('ok'):
                runs.append(res)
        self.record(db.backend, 'filter_existing_100k', impl.name, runs)

    def case_get_data(self, db: Db, impl: Impl) -> None:
        metric = defs.sync_metric('1M', impl.name)
        if not self._ensure_seed(db, impl, '1M'):
            self.record(db.backend, 'get_data_1M', impl.name, [{'ok': False, 'error': 'seed failed'}])
            return
        runs = []
        for k in range(1 + self.args.repeats):
            res = impl.worker('get_data', defs.SRC_INSTANCE, metric, defs.DEST_INSTANCE)
            if res.get('ok') and res.get('rows') != 1_000_000:
                res = {**res, 'ok': False, 'error': f'expected 1M rows, got {res.get("rows")}'}
            if k >= 1 or not res.get('ok'):
                runs.append(res)
        self.record(db.backend, 'get_data_1M', impl.name, runs)

    def case_latency(self, db: Db, impl: Impl) -> None:
        """Per-call latency on a small synced pipe: one fresh process per repeat."""
        metric = defs.latency_metric(impl.name)
        table = db.tbl(defs.target_name(metric))
        self._reset(db, impl, metric)
        seed = impl.sync_timed(defs.SRC_INSTANCE, metric)
        if not seed.get('ok') or db.count(table) != defs.LATENCY_ROWS:
            for call in LATENCY_CALLS:
                self.record(db.backend, f'lat_{call}', impl.name,
                            [{**seed, 'ok': False, 'error': 'seed failed'}])
            return
        _raw, _mid, leaf = defs.chain_metrics(impl.name)
        runs: Dict[str, List[Dict[str, Any]]] = {call: [] for call in LATENCY_CALLS}
        for _ in range(self.args.repeats):
            res = impl.worker(
                'latency', defs.SRC_INSTANCE, metric, defs.DEST_INSTANCE,
                str(self.args.latency_iterations), defs.DEST_INSTANCE, leaf,
            )
            for call in LATENCY_CALLS:
                if res.get('ok'):
                    runs[call].append({'ok': True, 'seconds': res['calls'][call]})
                else:
                    runs[call].append(res)
        for call in LATENCY_CALLS:
            self.record(db.backend, f'lat_{call}', impl.name, runs[call])

    def case_chain(self, db: Db, impl: Impl, engine: bool = False) -> None:
        raw, mid, leaf = defs.chain_metrics(impl.name)
        tables = [db.tbl(t) for t in defs.chain_targets(impl.name)]
        if engine:
            argv = ('sync', 'pipes', '-c', defs.DEST_INSTANCE, '-m', leaf, '-i', defs.DEST_INSTANCE,
                    '--parents', '--yes')
            case = 'chain_3_engine'
        else:
            argv = ('sync', 'pipes', '-t', defs.chain_tag(impl.name), '-i', defs.DEST_INSTANCE,
                    '-w', '1', '--yes')
            case = 'chain_3'

        def fn(_k):
            for t in tables:
                db.drop(t)
            impl.clear_cache()
            return impl.timed_cli(*argv)

        runs = self._run_timed(
            impl, fn, lambda _k: all(db.count(t) == 100_000 for t in tables), 1, self.args.repeats,
        )
        self.record(db.backend, case, impl.name, runs)


# -- report -------------------------------------------------------------------------------------

LATENCY_CALLS = (
    'first_call', 'get_sync_time', 'get_rowcount', 'get_data', 'fresh_get_sync_time',
    'template_get_sync_time',
)
CASE_ORDER = [
    'startup_import', 'startup_version', 'startup_show_version',
    'sync_10k', 'sync_100k', 'sync_1M', 'inc_100k', 'inc_1M',
    'filter_existing_100k', 'get_data_1M', 'chain_3', 'chain_3_engine',
] + [f'lat_{call}' for call in LATENCY_CALLS]
PAIRS = [('v5py', 'v4py'), ('v5cli', 'v4cli'), ('v5bin', 'v4cli')]
REGRESSION_THRESHOLD = 1.10  # v5 slower than v4 by more than 10%


def render_markdown(meta: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
    lines = [f'# Sync benchmark: {meta["label"]} ({meta["date"]})', '']
    lines += [
        f'- v4: meerschaum {meta["v4_version"]} (reference venv)',
        f'- v5: {meta["v5_version"]}',
        f'- Python {meta["python"]}, {meta["os"]}, {meta["cpus"]} CPUs',
        f'- measured interpreters: v4 Python {meta.get("v4_python", "unrecorded")}; '
        f'v5 Python {meta.get("v5_python", "unrecorded")}',
        f'- repeats: {meta["repeats"]}, including every 1M case (one extra warm-up up to 100k); '
        'the table shows the median seconds',
        '- `py` columns are timed around the call in-process; `cli` columns are whole-process '
        'wall time (startup included)',
        '- `v5cli` uses the installed wheel console script; `v5bin` uses the standalone release '
        'binary. Historical reports named the standalone binary `v5cli`.',
        '- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; '
        f'**bold** marks a regression (> {int((REGRESSION_THRESHOLD - 1) * 100)}% slower)',
        '`lat_*` cases are per call, in milliseconds',
        '',
    ]
    if meta.get('v5_extension_sha256'):
        lines += [f'- installed wheel extension SHA-256: `{meta["v5_extension_sha256"]}`',
                  f'- wheel console script SHA-256: `{meta.get("v5_console_sha256", "unrecorded")}`']
    if meta.get('v5_binary_sha256'):
        lines += [f'- standalone: {meta.get("v5_binary_version", "unrecorded")}; '
                  f'SHA-256 `{meta["v5_binary_sha256"]}`']
    lines.append('')
    regressions = []
    for backend in meta['backends']:
        lines += [f'## {backend}', '']
        lines += [
            '| case | v4py | v5py | x | v4cli | v5cli (wheel) | x | v5bin | x |',
            '|---|---|---|---|---|---|---|---|---|',
        ]
        by = {(r['case'], r['impl']): r for r in results if r['backend'] == backend}
        for case in CASE_ORDER:
            if not any((case, i) in by for i in defs.ALL_IMPLS):
                continue

            def cell(impl):
                r = by.get((case, impl))
                if r is None:
                    return 'n/a'
                if r['median'] is None:
                    return 'FAILED'
                if case.startswith('lat_'):
                    return f'{r["median"] * 1000:.2f} ms'
                return f'{r["median"]:.3f}'

            def ratio(v5, v4):
                a, b = by.get((case, v5)), by.get((case, v4))
                if not a or not b or a['median'] is None or not b['median']:
                    return ''
                x = a['median'] / b['median']
                if x > REGRESSION_THRESHOLD:
                    regressions.append((backend, case, v5, v4, x))
                    return f'**{x:.2f}**'
                return f'{x:.2f}'

            lines.append(
                f'| {case} | {cell("v4py")} | {cell("v5py")} '
                f'| {ratio("v5py", "v4py")} '
                f'| {cell("v4cli")} | {cell("v5cli")} | {ratio("v5cli", "v4cli")} '
                f'| {cell("v5bin")} | {ratio("v5bin", "v4cli")} |'
            )
        lines.append('')
    lines += ['## Regressions', '']
    if regressions:
        for backend, case, v5, v4, x in regressions:
            lines.append(f'- {backend} `{case}`: {v5} is {x:.2f}x {v4}')
    else:
        lines.append('None above the threshold.')
    lines += ['', '## Wave 10 comparison', '']
    comparisons = meta.get('baseline_regressions', [])
    if comparisons:
        for item in comparisons:
            lines.append(f'- {item["backend"]} `{item["case"]}` {item["impl"]}: '
                         f'{item["ratio"]:.2f}x wave 10 ({item["baseline"]}); confirm and profile')
    else:
        lines.append('No matched v5 case exceeded the 20% slowdown threshold. '
                     'The wheel CLI has no historical counterpart.')
    if meta.get('load_checks'):
        lines += ['', '## Machine load', '']
        for check in meta['load_checks']:
            lines.append(f'- {check["backend"]}: load averages {check["load_average"]}; '
                         f'rustc processes {check["rustc_processes"]}')
    failed = [r for r in results if r['median'] is None or r.get('failed')]
    if failed:
        lines += ['', '## Failed runs', '']
        for r in failed:
            lines.append(f'- {r["backend"]} `{r["case"]}` {r["impl"]}: {str(r.get("error", ""))[:160]!r}')
    if meta.get('notes'):
        lines += ['', '## Notes', ''] + [f'- {n}' for n in meta['notes']]
    lines.append('')
    return '\n'.join(lines)


def version_info(workdir: Path, snapshot: Path, implementations: List[str]) -> Dict[str, Any]:
    ref = Impl('v4py', workdir / 'versions', 'sqlite', snapshot)
    if ref.available():
        raise RuntimeError(ref.available())
    v4_info = ref.worker('identify')
    if not v4_info.get('ok') or v4_info.get('version') != '4.1.4':
        raise RuntimeError(f'Released v4.1.4 reference required: {v4_info}')
    v4 = v4_info['version']
    v5 = 'unavailable'
    if any(name.startswith('v5') for name in implementations):
        wheel = Impl('v5py', workdir / 'versions', 'sqlite', snapshot)
        v5_info = wheel.worker('identify')
        if not v5_info.get('ok') or not v5_info.get('version', '').startswith('5.'):
            raise RuntimeError(f'A v5 wheel environment is required: {v5_info}')
        if not v5_info.get('installed_wheel'):
            raise RuntimeError('--v5-python must use an installed wheel, not an editable/source tree')
        if not v5_info.get('extension_sha256'):
            raise RuntimeError('The installed native extension does not match the wheel RECORD')
        if v4_info['python'].split('.')[:2] != v5_info['python'].split('.')[:2]:
            raise RuntimeError('Use the same Python major/minor for both implementations')
        v5 = v5_info['version']
    commit = subprocess.run(['git', '-C', str(REPO), 'rev-parse', '--short', 'HEAD'],
                            capture_output=True, text=True).stdout.strip()
    info = {'v4_version': v4, 'v4_python': v4_info['python'], 'v5_version': v5,
            'driver_source_revision': commit, 'v4_dependencies': v4_info['dependencies']}
    harness = hashlib.sha256()
    for name in ('defs.py', 'run.py', 'worker.py'):
        harness.update((HERE / name).read_bytes())
    info['harness_sha256'] = harness.hexdigest()
    if any(name.startswith('v5') for name in implementations):
        info['v5_python'] = v5_info['python']
        info['v5_dependencies'] = v5_info['dependencies']
        info['v5_extension_sha256'] = v5_info['extension_sha256']
        info['v5_console_sha256'] = v5_info['console_sha256']
        info['v5_entry_module_sha256'] = v5_info['entry_module_sha256']
    def normalized(text):
        match = re.search(r'\d+\.\d+\.\d+(?:[-.]?(?:a|b|rc)[-.]?\d+)?', text)
        return re.sub(r'[-.]', '', match.group()) if match else ''

    if 'v5cli' in implementations:
        if not v5_info.get('console_sha256') or not v5_info.get('entry_module_sha256'):
            raise RuntimeError('Wheel mrsm must match RECORD, its selected interpreter and entry point')
        console = Impl('v5cli', workdir / 'versions', 'sqlite', snapshot)
        proc = console.run(console.cli_cmd('--version'))
        if proc.returncode or normalized(proc.stdout) != normalized(v5):
            raise RuntimeError('The wheel console script and installed package versions differ')
    if 'v5bin' in implementations:
        binary = Impl('v5bin', workdir / 'versions', 'sqlite', snapshot)
        proc = binary.run(binary.cli_cmd('--version'))
        if proc.returncode:
            raise RuntimeError('Standalone release binary version check failed')
        version = proc.stdout.strip().splitlines()[0]
        info['v5_binary_version'] = version
        with binary.mrsm_bin().open('rb') as stream:
            info['v5_binary_sha256'] = hashlib.file_digest(stream, 'sha256').hexdigest()
        if normalized(version) != normalized(v5):
            raise RuntimeError('Standalone release binary and installed wheel versions differ')
    return info


def expected_cells(args):
    """The measurements applicable to this selection, independent of subprocess outcomes."""
    cells = set()
    for impl in args.impls:
        python = impl in defs.PY_IMPLS
        if 'startup' in args.cases:
            names = ['startup_import'] if python else ['startup_version', 'startup_show_version']
            cells.update(('startup', name, impl) for name in names)
        for backend in args.backends:
            names = []
            if 'sync' in args.cases:
                names += [f'sync_{size}' for size in args.sizes]
            if 'inc' in args.cases:
                names += [f'inc_{size}' for size in args.sizes if size != '10k']
            if python:
                if 'filter_existing' in args.cases and '100k' in args.sizes:
                    names += ['filter_existing_100k']
                if 'get_data' in args.cases and '1M' in args.sizes:
                    names += ['get_data_1M']
                if 'latency' in args.cases:
                    names += [f'lat_{call}' for call in LATENCY_CALLS]
            elif 'chain' in args.cases:
                names += ['chain_3']
                if impl.startswith('v5'):
                    names += ['chain_3_engine']
            cells.update((backend, name, impl) for name in names)
    return cells


def merge_compatible(meta, results, previous):
    """Preserve per-run provenance: only identical artifacts and workloads can share a report."""
    keys = ('v4_version', 'v5_version', 'v4_python', 'v5_python', 'v4_dependencies',
            'v5_dependencies', 'v5_extension_sha256', 'v5_console_sha256', 'v5_entry_module_sha256',
            'v5_binary_version', 'v5_binary_sha256', 'repeats', 'sizes', 'cases',
            'implementations', 'profiling', 'latency_iterations',
            'driver_source_revision', 'harness_sha256')
    mismatches = [key for key in keys if meta.get(key) != previous['meta'].get(key)]
    if mismatches or not previous['meta'].get('complete'):
        raise ValueError('Existing label has incompatible or incomplete metadata: '
                         + ', '.join(mismatches or ['complete']))
    retained = [row for row in previous['results'] if row['backend'] not in meta['backends']]
    retained_backends = [b for b in previous['meta']['backends'] if b not in meta['backends']]
    meta['backends'] = retained_backends + meta['backends']
    meta['notes'] = previous['meta'].get('notes', []) + meta['notes']
    meta['load_checks'] = [row for row in previous['meta'].get('load_checks', [])
                          if row['backend'] in retained_backends] + meta['load_checks']
    return retained + results


def result_path(stem, suffix):
    """A dotted label is part of the filename, never a suffix to replace."""
    return Path(str(stem) + suffix)


def baseline_regressions(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map wave 10's standalone v5cli to v5bin without rewriting recorded baselines."""
    prior = {}
    for name in ('2026-10-15-w10-hardening-cli.json', '2026-10-15-w10-metadata.json'):
        path = HERE / 'results' / name
        if not path.exists():
            continue
        for row in json.loads(path.read_text()).get('results', []):
            impl = 'v5bin' if row['impl'] == 'v5cli' else row['impl']
            if impl in ('v5py', 'v5bin') and row.get('median') and not row.get('failed'):
                prior[(row['backend'], row['case'], impl)] = (row['median'], name)
    slow = []
    for row in results:
        previous = prior.get((row['backend'], row['case'], row['impl']))
        if previous and row.get('median') and row['median'] / previous[0] > 1.20:
            slow.append({key: row[key] for key in ('backend', 'case', 'impl')} |
                        {'ratio': row['median'] / previous[0], 'baseline': previous[1]})
    return slow


def scrub_public(value: Any, workdir: Path) -> Any:
    """Subprocess failures may contain paths or a URI; remove them before storing results."""
    if isinstance(value, str):
        for path, replacement in ((str(workdir), '[scratch]'), (str(REPO), '[repo]'),
                                  (str(Path.home()), '[home]')):
            value = value.replace(path, replacement)
        return re.sub(r'://[^\s/@:]+:[^\s/@]+@', '://[credentials]@', value)
    if isinstance(value, dict):
        return {k: scrub_public(v, workdir) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub_public(v, workdir) for v in value]
    return value


def main() -> int:
    global V5_PY, PROFILE_TRACE
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument('--label', default='dev', help='Host-agnostic label for the result files.')
    p.add_argument('--backends', default='sqlite,postgis,timescaledb')
    p.add_argument('--sizes', default='10k,100k,1M')
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--impls', default=','.join(defs.IMPLS))
    p.add_argument('--cases', default='sync,inc,filter_existing,get_data,chain,latency,startup')
    p.add_argument('--pg-schema', default=None,
                   help='Fresh scratch schema (must start with w17cbench_; default: unique).')
    p.add_argument('--v5-python', default=str(V5_PY),
                   help='Python in a dedicated environment holding the installed v5 wheel.')
    p.add_argument('--latency-iterations', type=int, default=200)
    p.add_argument('--workdir', default=None, help='Empty scratch directory below ~/.cache.')
    p.add_argument('--keep', action='store_true', help='Keep the temporary work directory.')
    p.add_argument('--plan', action='store_true', help='Print the workload without importing or running Meerschaum.')
    p.add_argument('--trace', action='store_true', help='Enable MRSM_CORE_TRACE for separate profiling runs.')
    args = p.parse_args()
    for attr in ('backends', 'sizes', 'impls', 'cases'):
        setattr(args, attr, [s for s in getattr(args, attr).split(',') if s])
    for name, choices in [('backends', ('sqlite', *defs.PG_BACKENDS)), ('sizes', defs.SIZES),
                          ('impls', defs.ALL_IMPLS),
                          ('cases', ('sync', 'inc', 'filter_existing', 'get_data', 'chain', 'latency', 'startup'))]:
        if not getattr(args, name) or set(getattr(args, name)) - set(choices):
            p.error(f'Invalid {name}; choose from {", ".join(choices)}')
        if len(getattr(args, name)) != len(set(getattr(args, name))):
            p.error(f'Duplicate {name} selections are not allowed')
    for case in args.cases:
        if not expected_cells(argparse.Namespace(**(vars(args) | {'cases': [case]}))):
            p.error(f'Case {case} does not apply to the selected implementations and sizes')
    if args.repeats < 1 or args.latency_iterations < 1:
        p.error('--repeats and --latency-iterations must be positive')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', args.label):
        p.error('--label must be a filename label without path separators')
    if args.pg_schema and not re.fullmatch(r'w17cbench_[a-z0-9_]{1,40}', args.pg_schema):
        p.error('--pg-schema must start with w17cbench_ and contain lowercase letters, digits or underscores')
    if args.plan:
        print(json.dumps({key: getattr(args, key) for key in ('backends', 'sizes', 'impls', 'cases', 'repeats')},
                         indent=2))
        return 0
    V5_PY = Path(args.v5_python).expanduser().absolute()
    PROFILE_TRACE = args.trace
    token = 'w17cbench_' + uuid.uuid4().hex[:12]
    defs.SCHEMA = args.pg_schema or token
    defs.PG_DATABASE = token
    cache = Path.home() / '.cache' / 'mrsm-bench'
    cache.mkdir(parents=True, exist_ok=True)
    workdir = Path(args.workdir).expanduser().absolute() if args.workdir else Path(
        tempfile.mkdtemp(prefix=token + '-', dir=cache))
    if not workdir.resolve().is_relative_to((Path.home() / '.cache').resolve()):
        p.error('--workdir must be below ~/.cache')
    if args.workdir and workdir.exists() and any(workdir.iterdir()):
        p.error('--workdir must be empty (a prior run must not supply stale artifacts or databases)')
    workdir.mkdir(parents=True, exist_ok=True)
    bench = Bench(args, workdir)
    versions = {}
    execution_error = None
    system_exit_code = None

    def metadata(complete):
        meta = {
            'label': args.label, 'date': datetime.date.today().isoformat(), 'backends': args.backends, 'repeats': args.repeats,
            'sizes': args.sizes, 'python': platform.python_version(), 'pg_schema': defs.SCHEMA,
            'pg_database': defs.PG_DATABASE,
            'os': f'{platform.system()} {platform.machine()}', 'cpus': os.cpu_count(),
            'notes': bench.notes, 'load_checks': bench.loads, 'complete': complete,
            'profiling': args.trace,
            'latency_iterations': args.latency_iterations,
            'implementations': args.impls, 'cases': args.cases,
            **{'v4_version': 'unavailable', 'v5_version': 'unavailable', **versions},
        }
        if 'startup' in args.cases:
            meta['backends'] = ['startup', *meta['backends']]
        return meta

    def checkpoint(stage):
        """Retain completed raw measurements even if a later backend cannot initialize."""
        meta = metadata(False)
        meta['checkpoint_after'] = stage
        meta['notes'] = [*meta['notes'], 'Checkpoint only; final workload and cleanup are not accepted.']
        directory = HERE / 'results'
        directory.mkdir(exist_ok=True)
        path = directory / f"{meta['date']}-{args.label}-checkpoint-{token}-{stage}.json"
        temporary = path.with_suffix('.json.tmp')
        payload = scrub_public({'meta': meta, 'results': bench.results}, workdir)
        with temporary.open('x') as stream:
            stream.write(json.dumps(payload, indent=2) + '\n')
        temporary.replace(path)
        print(f'wrote checkpoint {path}', flush=True)

    def interrupted(signum, _frame):
        raise SystemExit(128 + signum)

    for name in ('SIGTERM', 'SIGHUP'):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), interrupted)
    try:
        versions = version_info(workdir, bench.snapshot, args.impls)
        if set(args.backends) & {'postgis', 'timescaledb'}:
            for version in ('v4', 'v5'):
                if version == 'v5' and not any(impl.startswith('v5') for impl in args.impls):
                    continue
                if not versions.get(f'{version}_dependencies', {}).get('psycopg'):
                    raise RuntimeError(
                        f'{version} benchmark environment is missing psycopg; '
                        'provision the PostgreSQL DBAPI before starting measurements')
        if 'startup' in args.cases:
            bench.startup()
            checkpoint('startup')
        for backend in args.backends:
            bench.run_backend(backend)
            checkpoint(backend)
        measured = {(row['backend'], row['case'], row['impl']) for row in bench.results}
        if measured != expected_cells(args) or len(measured) != len(bench.results):
            raise RuntimeError('Measurement coverage differs from the requested workload')
    except (Exception, SystemExit) as e:
        execution_error = str(e)
        if isinstance(e, SystemExit):
            system_exit_code = e.code if isinstance(e.code, int) else 1
            execution_error = f'SystemExit: {e}'
        bench.notes.append(f'Run incomplete: {execution_error}')
    finally:
        for db in reversed(bench.databases):
            try:
                db.cleanup()
            except Exception as e:
                execution_error = f'scratch database cleanup failed: {e}'
                bench.notes.append(execution_error)
        if not args.keep and not args.workdir:
            shutil.rmtree(workdir, ignore_errors=True)

    today = datetime.date.today().isoformat()
    run_failed = execution_error is not None or any(
        r.get('failed') or r['median'] is None for r in bench.results)
    meta = metadata(not run_failed)
    out_dir = HERE / 'results'
    out_dir.mkdir(exist_ok=True)
    stem = out_dir / f'{today}-{args.label}'
    # Re-running some backends under an existing label replaces only those backends.
    previous = result_path(stem, '.json')
    results = bench.results
    if previous.exists() and not run_failed:
        try:
            results = merge_compatible(meta, results, json.loads(previous.read_text()))
        except (ValueError, KeyError, TypeError, OSError) as e:
            run_failed = True
            meta['notes'].append(str(e))
    run_failed = run_failed or any(row.get('failed') or row['median'] is None for row in results)
    merged_args = argparse.Namespace(**(vars(args) | {
        'backends': [backend for backend in meta['backends'] if backend != 'startup']}))
    measured = {(row['backend'], row['case'], row['impl']) for row in results}
    if measured != expected_cells(merged_args) or len(measured) != len(results):
        run_failed = True
        meta['notes'].append('Merged measurement coverage differs from the requested workload')
    meta['complete'] = not run_failed
    meta['baseline_regressions'] = baseline_regressions(results)
    meta = scrub_public(meta, workdir)
    bench.results = scrub_public(results, workdir)
    if run_failed and previous.exists():
        # Preserve a completed result when a rerun fails before measuring anything.
        stem = out_dir / f'{today}-{args.label}-incomplete-{uuid.uuid4().hex[:8]}'
    result_path(stem, '.json').write_text(
        json.dumps({'meta': meta, 'results': bench.results}, indent=2) + '\n'
    )
    result_path(stem, '.md').write_text(render_markdown(meta, bench.results))
    print(f'wrote {stem}.json and .md')
    # A premature SystemExit(0) is still an incomplete benchmark, never acceptance.
    return (system_exit_code or 1) if system_exit_code is not None else int(run_failed)


if __name__ == '__main__':
    sys.exit(main())
