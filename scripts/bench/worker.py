"""
Runs one benchmark operation inside an implementation's interpreter (released v3.x or v4.1.4, or
the installed v5 wheel). Prints one `RESULT {json}` line.

    worker.py setup   '<json list of pipe definitions>'
    worker.py sync    <connector> <metric> <instance>
    worker.py filter_existing <connector> <metric> <instance> <rows>
    worker.py get_data <connector> <metric> <instance>
    worker.py latency <connector> <metric> <instance> <iterations> <tmpl_connector> <tmpl_metric>

Only the measured call is timed (`seconds`); imports and process startup are excluded.
"""

from __future__ import annotations

import json
import os
import sys
import time


def record_identity(dist, path):
    """Verify an installed file against its distribution's RECORD without trusting its name."""
    import base64
    import hashlib
    from pathlib import Path
    path = Path(path)
    if not path.is_file():
        return None
    for entry in dist.files or []:
        if Path(dist.locate_file(entry)).resolve() != path.resolve() or entry.hash is None:
            continue
        digest = hashlib.new(entry.hash.mode, path.read_bytes()).digest()
        if base64.urlsafe_b64encode(digest).decode().rstrip('=') == entry.hash.value:
            return hashlib.sha256(path.read_bytes()).hexdigest()
    return None


def console_identity(dist, path, python):
    """A generated script must use this venv and this wheel's documented entry point."""
    from pathlib import Path
    path = Path(path)
    try:
        lines = path.read_text().splitlines()
    except (OSError, UnicodeError):
        return None
    if not lines or lines[0] != '#!' + str(Path(python).absolute()):
        return None
    if 'from _meerschaum_mrsm import main' not in [line.strip() for line in lines]:
        return None
    if not any(entry.name == 'mrsm' and entry.group == 'console_scripts'
               and entry.value == '_meerschaum_mrsm:main' for entry in dist.entry_points):
        return None
    return record_identity(dist, path)


def main() -> int:
    op = sys.argv[1]
    import meerschaum as mrsm

    info = {
        'version': mrsm.__version__,
        'python': sys.version.split()[0],
        'package': 'reference' if mrsm.__version__.startswith(('3.', '4.')) else 'wheel',
    }
    if op == 'identify':
        from importlib.metadata import distribution, version, PackageNotFoundError
        from pathlib import Path
        dist = distribution('meerschaum')
        metadata = dist.read_text('direct_url.json')
        direct = json.loads(metadata) if metadata else {}
        editable = direct.get('dir_info', {}).get('editable', False)
        in_repo = Path(mrsm.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[2])
        # The released references (3.x, 4.x) may live below the repository, but are identified
        # by version.
        identity = {'installed_wheel': not editable and not in_repo}
        identity['dependencies'] = {}
        for name in ('pandas', 'numpy', 'polars', 'pyarrow', 'SQLAlchemy', 'psycopg', 'psycopg2-binary'):
            try:
                identity['dependencies'][name] = version(name)
            except PackageNotFoundError:
                identity['dependencies'][name] = None
        if mrsm.__version__.startswith('5.'):
            from importlib.util import find_spec
            from meerschaum import _core
            extension = _core.load()
            identity['extension_sha256'] = record_identity(dist, extension.__file__) if extension else None
            identity['console_sha256'] = console_identity(
                dist, Path(sys.executable).parent / 'mrsm', sys.executable)
            entry_module = find_spec('_meerschaum_mrsm')
            identity['entry_module_sha256'] = record_identity(dist, entry_module.origin) if entry_module else None
        emit({'ok': True, **identity, **info})
        return 0
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
    if op == 'latency':
        return latency(pipe, info)
    if op == 'get_data':
        start = time.perf_counter()
        df = pipe.get_data()
        seconds = time.perf_counter() - start
        emit({'ok': True, 'seconds': seconds, 'rows': len(df), **info})
        return 0
    print(f'unknown op {op}', file=sys.stderr)
    return 2


def latency(pipe, info) -> int:
    """
    Per-call latency of the instance façade on a small synced pipe, in one process:

    - `first_call`: the process's first `get_rowcount()` (the connector, its pools and the
      pipe's metadata are built on the way), after the imports;
    - `get_sync_time` / `get_rowcount` / `get_data`: warm calls on one `Pipe` (median of
      `iterations`);
    - `fresh_get_sync_time`: a new `Pipe` object per call (metadata fetched each time);
    - `template_get_sync_time`: the leaf of the `{{ Pipe() }}` chain (its parameters resolve
      templates that reference two other pipes), a new `Pipe` per call.
    """
    import statistics
    import meerschaum as mrsm
    iterations = int(sys.argv[5])
    tmpl_connector, tmpl_metric = sys.argv[6:8]
    instance = sys.argv[4]

    start = time.perf_counter()
    rows = pipe.get_rowcount()
    first_call = time.perf_counter() - start

    def median_call(fn):
        for _ in range(3):
            fn()
        times = []
        for _ in range(iterations):
            t = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t)
        return statistics.median(times)

    keys = (pipe.connector_keys, pipe.metric_key)
    out = {
        'first_call': first_call,
        'get_sync_time': median_call(pipe.get_sync_time),
        'get_rowcount': median_call(pipe.get_rowcount),
        'get_data': median_call(pipe.get_data),
        'fresh_get_sync_time': median_call(
            lambda: mrsm.Pipe(*keys, instance=instance).get_sync_time()
        ),
        'template_get_sync_time': median_call(
            lambda: mrsm.Pipe(tmpl_connector, tmpl_metric, instance=instance).get_sync_time()
        ),
    }
    emit({'ok': rows is not None and rows > 0, 'rows': rows, 'calls': out, **info})
    return 0


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
