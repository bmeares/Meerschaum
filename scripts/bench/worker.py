"""
Runs one benchmark operation inside an implementation's interpreter (v4.1.4 from `.venv-ref`, or
the repository's `meerschaum` with `MRSM_CORE=rust`). Prints one `RESULT {json}` line.

    worker.py setup   '<json list of pipe definitions>'
    worker.py sync    <connector> <metric> <instance>
    worker.py filter_existing <connector> <metric> <instance> <rows>
    worker.py get_data <connector> <metric> <instance>

Only the measured call is timed (`seconds`); imports and process startup are excluded.
"""

from __future__ import annotations

import json
import os
import sys
import time


def main() -> int:
    op = sys.argv[1]
    import meerschaum as mrsm

    info = {
        'version': mrsm.__version__,
        'core': os.environ.get('MRSM_CORE', 'python'),
        'package': 'repo' if '.venv-ref' not in mrsm.__file__ else 'reference',
    }
    if op == 'setup':
        for d in json.loads(sys.argv[2]):
            pipe = mrsm.Pipe(
                d['connector'], d['metric'], instance=d['instance'], parameters=d['parameters'],
            )
            ok, msg = pipe.register(debug=False)
            if not ok:
                print(f'register failed for {pipe}: {msg}', file=sys.stderr)
                return 1
        emit({'ok': True, **info})
        return 0

    connector, metric, instance = sys.argv[2:5]
    pipe = mrsm.Pipe(connector, metric, instance=instance)
    if op == 'sync':
        start = time.perf_counter()
        success, msg = pipe.sync()
        seconds = time.perf_counter() - start
        emit({'ok': bool(success), 'seconds': seconds, 'message': str(msg)[:200], **info})
        return 0 if success else 1
    if op == 'filter_existing':
        rows = int(sys.argv[5])
        df = build_frame(rows)
        start = time.perf_counter()
        unseen, update, delta = pipe.filter_existing(df)
        seconds = time.perf_counter() - start
        emit({
            'ok': True, 'seconds': seconds, 'unseen': len(unseen), 'update': len(update),
            'delta': len(delta), **info,
        })
        return 0
    if op == 'get_data':
        start = time.perf_counter()
        df = pipe.get_data()
        seconds = time.perf_counter() - start
        emit({'ok': True, 'seconds': seconds, 'rows': len(df), **info})
        return 0
    print(f'unknown op {op}', file=sys.stderr)
    return 2


def build_frame(rows: int):
    """
    `rows` rows of the benchmark source layout: 99% identical to the first 99% of the synced
    table and 1% new (a later datetime range), so `filter_existing` has real work to do.
    """
    import numpy as np
    import pandas as pd
    from defs import BASE_EPOCH

    existing = rows - rows // 100
    idx = np.arange(rows, dtype='int64')
    seconds = np.where(idx < existing, idx, 10_000_000 + idx)
    dt = pd.to_datetime(BASE_EPOCH + seconds, unit='s', utc=True)
    return pd.DataFrame({
        'dt': dt,
        'id': (idx % 50).astype('int64'),
        'value': idx * 0.5,
    })


def emit(doc) -> None:
    print('RESULT ' + json.dumps(doc), flush=True)


if __name__ == '__main__':
    sys.exit(main())
