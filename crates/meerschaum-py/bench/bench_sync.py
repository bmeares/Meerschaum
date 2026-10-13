#! /usr/bin/env python3
# vim:fenc=utf-8

"""
End-to-end `Pipe` benchmark through the `SQLConnector` façade: `MRSM_CORE=python` (v4) vs
`MRSM_CORE=rust` (`meerschaum._core.SqlConnector`).

Run from the repository root with a release build of the extension (`scripts/rust/build_py.sh`):

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python3 crates/meerschaum-py/bench/bench_sync.py [--rows N] [--flavor sqlite postgis]
```

Both modes run in one process (the gate is flipped with `_gate.set_mode()`), each on its own
pipe so neither reads the other's table. Steps, timed once each (`get_data` best of
`--repeat`):

- `sync (new table)`: `pipe.sync(df)` of `N` rows into a pipe whose table does not exist
  (table creation, indices, bulk insert);
- `sync (half overlap)`: `pipe.sync(df2)` where half of `df2`'s rows are already stored
  (unchanged) and half are new (backtrack read, `filter_existing`, insert);
- `sync (update)`: `pipe.sync(df3)` changing the value of `--update-rows` stored rows
  (backtrack read, `filter_existing`, update through a temporary table);
- `get_data()`: read the whole table back into pandas;
- `get_sync_time()` and `get_rowcount()`.

The results of `get_data()` are compared between the modes (values and dtypes).

The `postgis` flavor uses the test container from `tests/connectors.py` (`localhost:5449`);
never run it while the pytest suite is running against the same database.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time

import numpy as np


def build_frames(rows: int, update_rows: int):
    import pandas as pd
    rng = np.random.default_rng(0)
    n = rows
    half = n // 2
    total = n + half
    dt = pd.date_range('2024-01-01', periods=total, freq='s', tz='UTC')
    full = pd.DataFrame({
        'dt': dt,
        'id': rng.integers(0, 1_000, total),
        'value': rng.random(total),
        'label': [f'label_{i % 997}' for i in range(total)],
    })
    first = full.iloc[:n].reset_index(drop=True)
    overlap = full.iloc[half:].reset_index(drop=True)
    stored = full.iloc[:total].reset_index(drop=True)
    update = stored.sample(n=max(1, update_rows), random_state=0).sort_values('dt').copy()
    update['value'] = update['value'] + 1.0
    update = update.reset_index(drop=True)
    return first, overlap, update


def get_conn(flavor: str, tmpdir: str):
    import meerschaum as mrsm
    if flavor == 'sqlite':
        return mrsm.get_connector(
            'sql', 'bench_sqlite', flavor='sqlite',
            database=os.path.join(tmpdir, 'bench.db'),
        )
    if flavor == 'postgis':
        return mrsm.get_connector(
            'sql', 'bench_postgis', flavor='postgis', username='test', password='test1234',
            database='testdb', port=5449, host='localhost', schema='public',
        )
    raise ValueError(f"Unsupported flavor '{flavor}'.")


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def run_mode(mode: str, conn, frames, repeat: int):
    import meerschaum as mrsm
    from meerschaum._core import _gate
    _gate.set_mode(mode)
    first, overlap, update = frames
    pipe = mrsm.Pipe(
        'bench', f'sync_{mode}', instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'value': 'float', 'label': 'string'},
    )
    pipe.delete()
    pipe = mrsm.Pipe(
        'bench', f'sync_{mode}', instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'value': 'float', 'label': 'string'},
    )
    times = {}
    t, res = timed(lambda: pipe.sync(first.copy()))
    assert res[0], res
    times['sync (new table)'] = t
    t, res = timed(lambda: pipe.sync(overlap.copy()))
    assert res[0], res
    times['sync (half overlap)'] = t
    t, res = timed(lambda: pipe.sync(update.copy()))
    assert res[0], res
    times['sync (update)'] = t
    best = None
    df = None
    for _ in range(repeat):
        t, df = timed(lambda: pipe.get_data())
        best = t if best is None else min(best, t)
    times['get_data()'] = best
    t, _ = timed(lambda: pipe.get_sync_time())
    times['get_sync_time()'] = t
    t, rc = timed(lambda: pipe.get_rowcount())
    times['get_rowcount()'] = t
    pipe.delete()
    _gate.set_mode('python')
    return times, df, rc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--repeat', type=int, default=3)
    parser.add_argument('--update-rows', type=int, default=10_000)
    parser.add_argument('--flavor', nargs='+', default=['sqlite', 'postgis'])
    parser.add_argument('--modes', nargs='+', default=['python', 'rust'])
    args = parser.parse_args()

    frames = build_frames(args.rows, args.update_rows)
    print(f"rows={args.rows:,}, update_rows={args.update_rows:,}")
    with tempfile.TemporaryDirectory() as tmpdir:
        for flavor in args.flavor:
            conn = get_conn(flavor, tmpdir)
            results = {}
            for mode in args.modes:
                results[mode] = run_mode(mode, conn, frames, args.repeat)
            print(f"\n## {flavor}\n")
            print('| Step | ' + ' | '.join(f'`{m}`' for m in args.modes) + ' | ratio |')
            print('|---|' + '---|' * len(args.modes) + '---|')
            for step in results[args.modes[0]][0]:
                vals = [results[m][0][step] for m in args.modes]
                ratio = (
                    f"{vals[-1] / vals[0]:.2f}x" if len(vals) == 2 and vals[0] else ''
                )
                print(f'| {step} | ' + ' | '.join(f'{v:.3f} s' for v in vals) + f' | {ratio} |')
            if len(args.modes) == 2:
                import pandas as pd
                (_, a, rc_a), (_, b, rc_b) = results[args.modes[0]], results[args.modes[1]]
                same_rc = rc_a == rc_b
                try:
                    pd.testing.assert_frame_equal(a, b, check_exact=True)
                    same = 'identical'
                except AssertionError as e:
                    same = f'DIFFERENT: {str(e).splitlines()[0:3]}'
                print(f"\nget_data() results: {same}; rowcounts {rc_a} / {rc_b}"
                      + ('' if same_rc else ' (DIFFERENT)'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
