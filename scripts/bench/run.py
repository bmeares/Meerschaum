#!/usr/bin/env python3
"""
Fixed sync benchmark set: Meerschaum v4.1.4 (the reference venv, `scripts/parity/setup_reference.sh`)
against v5 (the release `mrsm` binary and the Python façade).
Not part of CI. Spec §14 "Benchmarks": regressions block the RC.

    cargo build --release -p meerschaum --bin mrsm     # target/release/mrsm
    python scripts/bench/run.py --label rc1-dev      # all backends, sizes and cases

Implementations (columns of the report):

    v4py   v4.1.4, `pipe.sync()` etc. from Python, timed around the call (no interpreter startup)
    v4cli  v4.1.4, `python -m meerschaum sync pipes ...`, timed as a whole process
    v5py   this repository's `meerschaum` (release extension in meerschaum/_core)
    v5cli  target/release/mrsm, timed as a whole process

Cases (per backend: sqlite -> sqlite files, and postgis(5449) -> postgis, schema `w7_bench`
(`--pg-schema`) in the disposable database `w7_bench` of the test container):

    sync_{10k,100k,1M}   fresh destination, source SQL pipe (`sql:a_src` -> `sql:b_dest`)
    inc_{100k,1M}        destination already synced; 1% new source rows; time the re-sync
    filter_existing_100k Python only: `pipe.filter_existing` of a 100k frame (1% new)
    get_data_1M          Python only: `pipe.get_data()` of 1M rows
    chain_3              CLI only: `sync pipes -w 1` over raw -> mid -> leaf `{{ Pipe() }}` chain
    chain_3_engine       v5cli only: `sync pipes --parents` on the leaf of the same chain
    lat_*                Python only (case `latency`): per-call latency on a 50-row pipe, one
                         process per repeat: `lat_first_call` (the first `get_rowcount()` of
                         the process), warm `lat_get_sync_time` / `lat_get_rowcount` /
                         `lat_get_data` (median of `--latency-iterations` calls), and a new `Pipe`
                         per call for `lat_fresh_get_sync_time` and for the `{{ Pipe() }}` chain
                         leaf (`lat_template_get_sync_time`)

Every run uses a fresh temporary MRSM_ROOT_DIR per implementation, an empty plugins dir, and
only the `w7_bench` schema/database. One warm-up (a 10k sync) per implementation and backend,
then one extra warm-up repeat per case for sizes up to 100k, then `--repeats` timed repeats
(at most 2 for 1M).

Output: scripts/bench/results/<date>-<label>.{json,md}. No hostnames, paths or environment are
recorded.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE))
import defs  # noqa: E402

REF_PY = REPO / '.venv-ref' / 'bin' / 'python'
MRSM_BIN = REPO / 'target' / 'release' / 'mrsm'
REF_VENVS = REPO / '.venv-ref' / 'mrsm_root' / 'venvs'
RUN_TIMEOUT = 3600


class Db:
    """Raw SQL access to one backend, used only to prepare and verify (never timed)."""

    def __init__(self, backend: str, workdir: Path):
        self.backend = backend
        self.workdir = workdir

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

    def _psql(self, sql: str, tuples: bool = False) -> str:
        cmd = [
            'psql', '-h', 'localhost', '-p', '5449', '-U', 'test', '-d', defs.PG_DATABASE,
            '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-c', sql,
        ]
        if tuples:
            cmd += ['-t', '-A']
        proc = subprocess.run(
            cmd, env={**os.environ, 'PGPASSWORD': 'test1234'}, capture_output=True, text=True,
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
        if self.name == 'v5cli' and not self.mrsm_bin().exists():
            return 'missing target/release/mrsm (cargo build --release -p meerschaum --bin mrsm)'
        return None

    def mrsm_bin(self) -> Path:
        return self.snapshot / 'mrsm' if self.snapshot else MRSM_BIN

    def env(self) -> Dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith(('MRSM_', 'PYTHON'))}
        env.update({
            'MRSM_ROOT_DIR': str(self.root),
            'MRSM_PLUGINS_DIR': str(self.plugins),
            'MRSM_VENVS_DIR': str(REF_VENVS),
            'UV_PYTHON': str(REF_PY),
            'PYTHONHASHSEED': '0',
        })
        env.update(defs.connector_env(self.backend, str(self.workdir)))
        if self.name == 'v5py':
            env['PYTHONPATH'] = str(self.snapshot / 'py' if self.snapshot else REPO)
        env['MRSM_BENCH_PG_SCHEMA'] = defs.SCHEMA
        return env

    def py_cmd(self, *args: str) -> List[str]:
        worker = str(HERE / 'worker.py')
        if self.name.startswith('v4'):
            return [str(REF_PY), '-s', '-E', worker, *args]
        return [sys.executable, worker, *args]

    def cli_cmd(self, *args: str) -> List[str]:
        if self.name == 'v4cli':
            return [str(REF_PY), '-s', '-E', '-m', 'meerschaum', *args]
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
                return json.loads(line[len('RESULT '):])
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
        self.snapshot = self._snapshot()

    def _snapshot(self) -> Path:
        """Copy the v5 binary and Python package so a concurrent rebuild cannot change them mid-run."""
        snap = self.workdir / 'v5snapshot'
        if snap.exists():
            return snap
        (snap / 'py').mkdir(parents=True)
        if MRSM_BIN.exists():
            shutil.copy2(MRSM_BIN, snap / 'mrsm')
        shutil.copytree(
            REPO / 'meerschaum', snap / 'py' / 'meerschaum',
            ignore=shutil.ignore_patterns('__pycache__', '*.pyc'),
        )
        return snap

    # -- helpers ----------------------------------------------------------------------------

    def log(self, msg: str) -> None:
        print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)

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
        reps = self.args.repeats if defs.SIZES[label] < 1_000_000 else min(2, self.args.repeats)
        return warm, reps

    # -- one backend ------------------------------------------------------------------------

    def run_backend(self, backend: str) -> None:
        a = self.args
        wd = self.workdir / backend
        wd.mkdir(parents=True, exist_ok=True)
        db = Db(backend, wd)
        labels = [lab for lab in defs.SIZES if lab in a.sizes]
        needed = sorted(set(labels) | {'10k', '100k'}, key=lambda s: defs.SIZES[s])
        impls = [Impl(n, wd, backend, self.snapshot) for n in a.impls]
        impls = [i for i in impls if not self._unavailable(i)]
        if not impls:
            return
        self.log(f'== {backend}: preparing sources {needed}')
        if backend == 'postgis':
            db.exec(f'DROP SCHEMA IF EXISTS {defs.SCHEMA} CASCADE')
            db.exec(f'CREATE SCHEMA {defs.SCHEMA}')
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
        if 'chain' in a.cases and 'v5cli' in [i.name for i in impls]:
            self.case_chain(db, next(i for i in impls if i.name == 'v5cli'), engine=True)

    def _unavailable(self, impl: Impl) -> bool:
        why = impl.available()
        if why:
            self.notes.append(f'{impl.name} skipped: {why}')
            self.log(f'{impl.name} skipped: {why}')
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
        for k in range(1 + min(2, self.args.repeats)):
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
    'sync_10k', 'sync_100k', 'sync_1M', 'inc_100k', 'inc_1M',
    'filter_existing_100k', 'get_data_1M', 'chain_3', 'chain_3_engine',
] + [f'lat_{call}' for call in LATENCY_CALLS]
PAIRS = [('v5py', 'v4py'), ('v5cli', 'v4cli')]
REGRESSION_THRESHOLD = 1.10  # v5 slower than v4 by more than 10%


def render_markdown(meta: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
    lines = [f'# Sync benchmark: {meta["label"]} ({meta["date"]})', '']
    lines += [
        f'- v4: meerschaum {meta["v4_version"]} (reference venv)',
        f'- v5: {meta["v5_version"]}',
        f'- Python {meta["python"]}, {meta["os"]}, {meta["cpus"]} CPUs',
        f'- repeats: {meta["repeats"]} (1 extra warm-up per case up to 100k; 1M uses at most 2); '
        'the table shows the median seconds',
        '- `py` columns are timed around the call in-process; `cli` columns are whole-process '
        'wall time (startup included)',
        '- `x` columns: v5 time / v4 time for the same style (`py`, `cli`). Below 1.00 is faster; '
        f'**bold** marks a regression (> {int((REGRESSION_THRESHOLD - 1) * 100)}% slower)',
        '`lat_*` cases are per call, in milliseconds',
        '',
    ]
    regressions = []
    for backend in meta['backends']:
        lines += [f'## {backend}', '']
        lines += [
            '| case | v4py | v5py | x | v4cli | v5cli | x |',
            '|---|---|---|---|---|---|---|',
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
                if not a or not b or a['median'] is None or b['median'] is None:
                    return ''
                x = a['median'] / b['median']
                if x > REGRESSION_THRESHOLD:
                    regressions.append((backend, case, v5, v4, x))
                    return f'**{x:.2f}**'
                return f'{x:.2f}'

            lines.append(
                f'| {case} | {cell("v4py")} | {cell("v5py")} '
                f'| {ratio("v5py", "v4py")} '
                f'| {cell("v4cli")} | {cell("v5cli")} | {ratio("v5cli", "v4cli")} |'
            )
        lines.append('')
    lines += ['## Regressions', '']
    if regressions:
        for backend, case, v5, v4, x in regressions:
            lines.append(f'- {backend} `{case}`: {v5} is {x:.2f}x {v4}')
    else:
        lines.append('None above the threshold.')
    failed = [r for r in results if r['median'] is None]
    if failed:
        lines += ['', '## Failed runs', '']
        for r in failed:
            lines.append(f'- {r["backend"]} `{r["case"]}` {r["impl"]}: {str(r.get("error", ""))[:160]!r}')
    if meta.get('notes'):
        lines += ['', '## Notes', ''] + [f'- {n}' for n in meta['notes']]
    lines.append('')
    return '\n'.join(lines)


def version_info() -> Dict[str, str]:
    v4 = 'unavailable'
    if REF_PY.exists():
        proc = subprocess.run(
            [str(REF_PY), '-s', '-E', '-c', 'import meerschaum; print(meerschaum.__version__)'],
            capture_output=True, text=True, cwd=tempfile.gettempdir(),
        )
        v4 = proc.stdout.strip() or v4
    v5 = 'unavailable'
    if MRSM_BIN.exists():
        proc = subprocess.run([str(MRSM_BIN), '--version'], capture_output=True, text=True,
                              env={**os.environ, 'MRSM_ROOT_DIR': tempfile.mkdtemp()})
        v5 = (proc.stdout.strip() or v5).splitlines()[0] if proc.stdout.strip() else v5
    commit = subprocess.run(['git', '-C', str(REPO), 'rev-parse', '--short', 'HEAD'],
                            capture_output=True, text=True).stdout.strip()
    return {'v4_version': v4, 'v5_version': f'`mrsm` release build ({v5}), façade at {commit} + working tree'}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument('--label', default='dev', help='Host-agnostic label for the result files.')
    p.add_argument('--backends', default='sqlite,postgis')
    p.add_argument('--sizes', default='10k,100k,1M')
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--impls', default=','.join(defs.IMPLS))
    p.add_argument('--cases', default='sync,inc,filter_existing,get_data,chain,latency')
    p.add_argument('--pg-schema', default=defs.SCHEMA,
                   help='Schema of the postgis backend (in the w7_bench database).')
    p.add_argument('--latency-iterations', type=int, default=200)
    p.add_argument('--workdir', default=None, help='Default: a fresh temporary directory.')
    p.add_argument('--keep', action='store_true', help='Keep the temporary work directory.')
    args = p.parse_args()
    for attr in ('backends', 'sizes', 'impls', 'cases'):
        setattr(args, attr, [s for s in getattr(args, attr).split(',') if s])
    defs.SCHEMA = args.pg_schema

    workdir = Path(args.workdir or tempfile.mkdtemp(prefix='mrsm-bench-'))
    workdir.mkdir(parents=True, exist_ok=True)
    bench = Bench(args, workdir)
    try:
        for backend in args.backends:
            bench.run_backend(backend)
    finally:
        if not args.keep and not args.workdir:
            shutil.rmtree(workdir, ignore_errors=True)
        if 'postgis' in args.backends:
            try:
                Db('postgis', workdir).exec(f'DROP SCHEMA IF EXISTS {defs.SCHEMA} CASCADE')
            except Exception as e:
                print(f'schema cleanup failed: {e}', file=sys.stderr)

    today = datetime.date.today().isoformat()
    meta = {
        'label': args.label, 'date': today, 'backends': args.backends, 'repeats': args.repeats,
        'sizes': args.sizes, 'python': platform.python_version(), 'pg_schema': defs.SCHEMA,
        'os': f'{platform.system()} {platform.machine()}', 'cpus': os.cpu_count(),
        'notes': bench.notes, **version_info(),
    }
    out_dir = HERE / 'results'
    out_dir.mkdir(exist_ok=True)
    stem = out_dir / f'{today}-{args.label}'
    # Re-running some backends under an existing label replaces only those backends.
    previous = stem.with_suffix('.json')
    results = bench.results
    if previous.exists():
        old = json.loads(previous.read_text())
        kept = [r for r in old['results'] if r['backend'] not in args.backends]
        results = kept + bench.results
        meta['backends'] = [b for b in old['meta']['backends'] if b not in args.backends] + args.backends
        meta['notes'] = old['meta'].get('notes', []) + bench.notes
    bench.results = results
    (stem.with_suffix('.json')).write_text(
        json.dumps({'meta': meta, 'results': bench.results}, indent=2) + '\n'
    )
    (stem.with_suffix('.md')).write_text(render_markdown(meta, bench.results))
    print(f'wrote {stem}.json and .md')
    return 0


if __name__ == '__main__':
    sys.exit(main())
