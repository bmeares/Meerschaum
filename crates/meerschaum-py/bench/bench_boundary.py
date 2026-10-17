#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Time the kernel bindings of `meerschaum._core` against the v4 Python implementations.

Run from the repository root with a release build of the extension (`scripts/rust/build_py.sh`):

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python3 crates/meerschaum-py/bench/bench_boundary.py [--rows N] [--repeat 3] [--only NAME ...]
```

Both modes run in one process: the `MRSM_CORE` gate is flipped with `_gate.set_mode()` between
calls, so the Rust path goes through exactly the guard the test suite uses. Each call is timed
best-of-`--repeat`, and the two results are compared (values and dtypes) so a speed-up can never
silently change an output.

`--profile NAME --mode rust --loops K` runs one scenario `K` times without timing, for
`py-spy record --native`.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from decimal import Decimal

import numpy as np


def build_scenarios(rows: int, special_rows: int):
    """Return `{name: (callable_factory, description)}`; each factory returns a zero-arg call."""
    import pandas as pd
    from meerschaum.utils.dataframe import enforce_dtypes, filter_unseen_df

    rng = np.random.default_rng(0)
    n = rows
    half = n // 2
    dt = pd.date_range('2024-01-01', periods=n + half, freq='s', tz='UTC')
    ids = rng.integers(0, 1_000, n + half)
    values = rng.random(n + half)
    labels = pd.array([f'label_{i % 997}' for i in range(n + half)], dtype='string')
    full = pd.DataFrame({'dt': dt, 'id': ids, 'value': values, 'label': labels})
    old = full.iloc[:n].reset_index(drop=True)
    # Half of `new` overlaps `old` (same rows), half is unseen.
    new = full.iloc[half:].reset_index(drop=True)

    strings = pd.DataFrame({
        'dt': old['dt'].dt.strftime('%Y-%m-%d %H:%M:%S').astype(object),
        'id': old['id'].astype(str).astype(object),
        'value': old['value'].astype(str).astype(object),
    })

    # 100k rows of fallback (object) columns.
    from meerschaum.utils.packages import attempt_import
    shapely = attempt_import('shapely', lazy=False)
    m = special_rows
    srng = np.random.default_rng(1)
    xs = srng.random(m)
    special = pd.DataFrame({
        'dt': pd.date_range('2024-01-01', periods=m, freq='min', tz='UTC'),
        'id': srng.integers(0, 100, m),
        'meta': [{'a': int(i), 'b': [float(x), 'x']} for i, x in zip(range(m), xs)],
        'guid': [uuid.UUID(int=int(i) * 7919 + 1) for i in range(m)],
        'amount': [Decimal(f'{x:.6f}') for x in xs],
        'blob': [bytes([i % 256, (i * 3) % 256, 7]) for i in range(m)],
        'geom': [shapely.Point(float(x), float(i % 90)) for i, x in zip(range(m), xs)],
    })
    special_old = special.iloc[: m // 2 + m // 4].reset_index(drop=True)
    special_new = special.iloc[m // 4:].reset_index(drop=True)
    special_dtypes = {
        'dt': 'datetime64[ns, UTC]',
        'id': 'int',
        'meta': 'json',
        'guid': 'uuid',
        'amount': 'numeric',
        'blob': 'bytes',
        'geom': 'geometry',
    }

    return {
        'filter_unseen_df': (
            lambda: filter_unseen_df(old, new),
            f'filter_unseen_df(old, new), {n:,} rows each, half overlapping',
        ),
        'enforce_all_string': (
            lambda: enforce_dtypes(
                strings, {'dt': 'datetime', 'id': 'int', 'value': 'float'},
            ),
            f'enforce_dtypes(all-string frame, {{dt, id, value}}), {n:,} rows (whole-frame path)',
        ),
        'enforce_mixed': (
            lambda: enforce_dtypes(old, {'id': 'int'}),
            f'enforce_dtypes(typed frame, {{id: int}}), {n:,} rows (native subset + untyped)',
        ),
        'enforce_special': (
            lambda: enforce_dtypes(special, special_dtypes),
            f'enforce_dtypes(json/uuid/numeric/bytes/geometry), {m:,} rows',
        ),
        'filter_special': (
            lambda: filter_unseen_df(special_old, special_new, dtypes=special_dtypes),
            f'filter_unseen_df(json/uuid/numeric/bytes/geometry), {m * 3 // 4:,} rows each',
        ),
    }


def set_mode(mode: str) -> None:
    from meerschaum._core import _gate
    _gate.set_mode(mode)


def best_of(func, repeat: int):
    best = float('inf')
    result = None
    for _ in range(repeat):
        start = time.perf_counter()
        result = func()
        best = min(best, time.perf_counter() - start)
    return best, result


def compare(py_df, rs_df) -> str:
    """'ok' when the two frames are identical (values, dtypes, index, class), else a reason."""
    import pandas as pd
    if type(py_df) is not type(rs_df):
        return f'class {type(py_df).__name__} != {type(rs_df).__name__}'
    py_dtypes = {str(c): str(t) for c, t in py_df.dtypes.items()}
    rs_dtypes = {str(c): str(t) for c, t in rs_df.dtypes.items()}
    if py_dtypes != rs_dtypes:
        diff = {c: (py_dtypes.get(c), rs_dtypes.get(c)) for c in py_dtypes if py_dtypes.get(c) != rs_dtypes.get(c)}
        return f'dtypes differ: {diff}'
    try:
        pd.testing.assert_frame_equal(py_df, rs_df, check_exact=True)
    except AssertionError as e:
        return 'values differ: ' + str(e).splitlines()[0]
    return 'ok'


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--special-rows', type=int, default=100_000)
    parser.add_argument('--repeat', type=int, default=3)
    parser.add_argument('--only', nargs='*')
    parser.add_argument('--modes', nargs='*', default=['python', 'rust'])
    parser.add_argument('--profile', help='Run one scenario in a loop (for py-spy).')
    parser.add_argument('--mode', default='rust')
    parser.add_argument('--loops', type=int, default=5)
    args = parser.parse_args()

    import meerschaum._core as core
    if not core.AVAILABLE:
        print(f'meerschaum._core is not built: {core.LOAD_ERROR!r}', file=sys.stderr)
        return 1
    info = core.build_info()
    if info['profile'] != 'release':
        print(f"warning: extension built in {info['profile']} mode", file=sys.stderr)

    scenarios = build_scenarios(args.rows, args.special_rows)

    if args.profile:
        call, _ = scenarios[args.profile]
        set_mode(args.mode)
        call()
        start = time.perf_counter()
        for _ in range(args.loops):
            call()
        print(f'{args.profile} [{args.mode}] x{args.loops}: {time.perf_counter() - start:.3f} s')
        return 0

    names = args.only or list(scenarios)
    rows = []
    for name in names:
        call, desc = scenarios[name]
        times = {}
        results = {}
        for mode in args.modes:
            set_mode(mode)
            call()  # warm-up (imports, caches)
            times[mode], results[mode] = best_of(call, args.repeat)
        status = (
            compare(results['python'], results['rust'])
            if {'python', 'rust'} <= set(results) else '-'
        )
        rows.append((name, desc, times, status))
        set_mode('python')

    print(f"\nrelease={info['profile']}  rows={args.rows:,}  special_rows={args.special_rows:,}  "
          f"best of {args.repeat}\n")
    header = f"| {'scenario':<20} | " + ' | '.join(f'{m:>8}' for m in args.modes)
    if len(args.modes) == 2:
        header += ' | rust/python | result'
    print(header + ' |')
    print('|' + '|'.join('-' * len(c) for c in header.split('|')[1:]) + '---|')
    for name, desc, times, status in rows:
        line = f'| {name:<20} | ' + ' | '.join(f'{times[m]:7.3f}s' for m in args.modes)
        if len(args.modes) == 2:
            ratio = times['rust'] / times['python']
            line += f' | {ratio:10.2f}x | {status}'
        print(line + ' |')
    print()
    for name, desc, _, _ in rows:
        print(f'  {name}: {desc}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
